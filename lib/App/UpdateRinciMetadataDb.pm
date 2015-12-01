package App::UpdateRinciMetadataDb;

# DATE
# VERSION

use 5.010001;
use strict;
use warnings;
use experimental 'smartmatch';
use Log::Any::IfLOG '$log';

use Data::Clean::JSON;
use DBI;
use JSON;
use Module::Load qw(autoload load);
use Module::Path::More;
use Perinci::Access::Perl;
use SQL::Schema::Versioned;

use Data::Clean::JSON;

my $cleanser = Data::Clean::JSON->get_cleanser;

our %SPEC;

sub _is_excluded {
    my ($x, $exc_list) = @_;
    for (@$exc_list) {
        if (/(.+)::\*?\z/) {
            return 1 if index($x, "$1\::") == 0;
        } else {
            return 1 if $x eq $_;
        }
    }
    0;
}

our $db_schema_spec = {
    latest_v => 3,
    install => [
        'CREATE TABLE IF NOT EXISTS package (name VARCHAR(255) PRIMARY KEY, summary TEXT, metadata BLOB, dist TEXT, extra TEXT, mtime INT)',
        'CREATE TABLE IF NOT EXISTS function (package VARCHAR(255) NOT NULL, name VARCHAR(255) NOT NULL, summary TEXT, metadata BLOB, extra TEXT, UNIQUE(package, name))',
    ],
    upgrade_to_v3 => [
        'ALTER TABLE package ADD COLUMN dist TEXT',
        'ALTER TABLE package ADD COLUMN extra TEXT', # a column to store random extra stuffs
        'ALTER TABLE function ADD COLUMN extra TEXT', # a column to store random extra stuffs
    ],
    upgrade_to_v2 => [
        # rename to package
        'DROP TABLE module',
        'CREATE TABLE IF NOT EXISTS package (name VARCHAR(255) PRIMARY KEY, summary TEXT, metadata BLOB, mtime INT)',

        # we'll just drop everything and rebuild, since it's painful to
        # rename column in sqlite
        'DROP TABLE function',
        'CREATE TABLE IF NOT EXISTS function (package VARCHAR(255) NOT NULL, name VARCHAR(255) NOT NULL, summary TEXT, metadata BLOB, UNIQUE(package, name))',
    ],

    # for testing
    install_v1 => [
        'CREATE TABLE IF NOT EXISTS module (name VARCHAR(255) PRIMARY KEY, summary TEXT, metadata BLOB, mtime INT)',
        'CREATE TABLE IF NOT EXISTS function (module VARCHAR(255) NOT NULL, name VARCHAR(255) NOT NULL, summary TEXT, metadata BLOB, UNIQUE(module, name))',
    ],
    install_v2 => [
        'CREATE TABLE IF NOT EXISTS package (name VARCHAR(255) PRIMARY KEY, summary TEXT, metadata BLOB, mtime INT)',
        'CREATE TABLE IF NOT EXISTS function (package VARCHAR(255) NOT NULL, name VARCHAR(255) NOT NULL, summary TEXT, metadata BLOB, UNIQUE(package, name))',
    ],
};

$SPEC{update_rinci_metadata_db} = {
    v => 1.1,
    summary => 'Create/update Rinci API metadata database',
    args => {
        dsn => {
            summary => 'DBI connection DSN',
            description => <<'_',

Note: has been tested with MySQL and SQLite only.

_
            schema => 'str*',
            req => 1,
            pos => 0,
        },
        user => {
            summary => 'DBI connection user',
            schema => 'str*',
        },
        password => {
            summary => 'DBI connection password',
            schema => 'str*',
        },
        module_or_package => {
            summary => 'Perl module or prefixes or package to add/update',
            description => <<'_',

For each entry, you can specify:

* a Perl module name e.g. `Foo::Bar`. An attempt will be made to load that
  module.

* a module prefix ending with `::` or `::*` e.g. `Foo::Bar::*`. `Module::List`
  will be used to list all modules under `Foo::Bar::` recursively and load all
  those modules.

* a package name using `+Foo::Bar` syntax. An attempt to load module with that
  name will *not* be made. This can be used to add an already-loaded package
  e.g. by another module).

* a package prefix using `+Foo::Bar::` or `+Foo::Bar::*` syntax. Subpackages
  will be listed recursively (using `Package::MoreUtil`'s
  `list_subpackages`).

_
            schema => ['array*' => of => 'str*'],
            req => 1,
            pos => 1,
            greedy => 1,
        },
        exclude => {
            summary => 'Perl package names or prefixes to exclude',
            schema => ['array*' => of => 'str*'],
        },
        library => {
            summary => "Include library path, like Perl's -I",
            schema => 'str*',
            description => <<'_',

Note that some modules are already loaded before this option takes effect. To
make sure you use the right library, you can use `PERL5OPT` or explicitly use
`perl` and use its `-I` option.

_
            cmdline_aliases => { I=>{} },
            cmdline_on_getopt => sub {
                my %args = @_;
                require lib;
                lib->import($args{value});
            },
        },
        use => {
            schema => ['array' => of => 'str*'],
            summary => 'Use a Perl module, a la Perl\'s -M',
            cmdline_aliases => {M=>{}},
            cmdline_on_getopt => sub {
                my %args = @_;
                my $val = $args{value};
                if (my ($mod, $imp) = $val =~ /(.+?)=(.+)/) {
                    $log->debug("Loading module $mod ...");
                    load $mod;
                    $mod->import(split /,/, $imp);
                } else {
                    $log->debug("Loading module $val ...");
                    autoload $val;
                }
            },
        },
        require => {
            schema => ['array' => of => 'str*'],
            summary => 'Require a Perl module, a la Perl\'s -m',
            cmdline_aliases => {m=>{}},
            cmdline_on_getopt => sub {
                my %args = @_;
                my $val = $args{value};
                $log->debug("Loading module $val ...");
                load $val;
            },
        },
        force_update => {
            summary => "Force update database even though module ".
                "hasn't changed since last update",
            schema => 'bool',
            cmdline_aliases => { force=>{} }, # old alias
        },
        delete => {
            summary => "Whether to delete packages from DB if no longer ".
                "mentioned as arguments or found in filesystem",
            schema  => 'bool',
            default => 1,
        },
    },
    features => {
        progress => 1,
        dry_run => 1,
    },
};
sub update_rinci_metadata_db {
    my %args = @_;

    require DBI;
    require JSON;
    require Module::List;
    require Package::MoreUtil;
    require Perinci::Access::Perl;
    require SQL::Schema::Versioned;

    state $json = JSON->new->allow_nonref;
    state $pa = Perinci::Access::Perl->new;

    my $dbh = DBI->connect($args{dsn}, $args{user}, $args{password},
                           {RaiseError=>1});

    my $res = SQL::Schema::Versioned::create_or_update_db_schema(
        spec => $db_schema_spec,
        dbh => $dbh,
    );
    return $res unless $res->[0] == 200;

    my $exc = $args{exclude} // [];

    my @pkgs;
    for my $entry (@{ $args{module_or_package} }) {
        if ($entry =~ /\A\+(.+)::\*?\z/) {
            # package prefix
            $log->debug("Listing all packages under $1 ...");
            for (Package::MoreUtil::list_subpackages($1, 1)) {
                next if $_ ~~ @pkgs || _is_excluded($_, $exc);
                push @pkgs, $_;
            }
        } elsif ($entry =~ /\A\+(.+)/) {
            # package name
            my $pkg = $1;
            next if $pkg ~~ @pkgs || _is_excluded($pkg, $exc);
            push @pkgs, $pkg;
        } elsif ($entry =~ /(.+::)\*?\z/) {
            # module prefix
            $log->debug("Listing all modules under $1 ...");
            my $res = Module::List::list_modules(
                $1, {list_modules=>1, recurse=>1});
            for my $mod (sort keys %$res) {
                next if $mod ~~ @pkgs || _is_excluded($mod, $exc);
                $log->debug("Loading module $mod ...");
                load $mod;
                push @pkgs, $mod;
            }
        } else {
            # module name
            next if $entry ~~ @pkgs || _is_excluded($entry, $exc);
            $log->debug("Loading module $entry ...");
            load $entry;
            push @pkgs, $entry;
        }
    }

    my $progress = $args{-progress};
    $progress->pos(0) if $progress;
    $progress->target(~~@pkgs) if $progress;
    my $i = 0;
    for my $pkg (@pkgs) {
        $i++;
        $progress->update(pos=>$i, message => "Processing package $pkg ...") if $progress;
        $log->debug("Processing package $pkg ...");
        #sleep 1;
        my $rec = $dbh->selectrow_hashref("SELECT * FROM package WHERE name=?",
                                          {}, $pkg);
        my $mp = Module::Path::More::module_path(module=>$pkg);
        my @st = stat($mp) if $mp;

        unless ($args{force} || !$rec || !$rec->{mtime} || !@st || $rec->{mtime} < $st[9]) {
            $log->debug("$pkg ($mp) hasn't changed since last recorded, skipped");
            next;
        }

        next if $args{-dry_run};

        my $uri = $pkg; $uri =~ s!::!/!g; $uri = "pl:/$uri/";

        $res = $pa->request(meta => "$uri");
        die "Can't meta $uri: $res->[0] - $res->[1]" unless $res->[0] == 200;
        $cleanser->clean_in_place(my $pkgmeta = $res->[2]);

        $res = $pa->request(list => $uri, {type=>'function'});
        die "Can't list $uri: $res->[0] - $res->[1]" unless $res->[0] == 200;
        my $numf = @{ $res->[2] };

        $dbh->do("INSERT INTO package (name, summary, metadata, mtime) VALUES (?,?,?,0)", {}, $pkg, $pkgmeta->{summary}, $json->encode($pkgmeta), $st[9]) unless $rec;
        $dbh->do("UPDATE package set mtime=? WHERE name=?", {}, $st[9], $pkg);
        $dbh->do("DELETE FROM function WHERE package=?", {}, $pkg);
        my $j = 0;
        for my $e (@{ $res->[2] }) {
            my $f = $e; $f =~ s!.+/!!;
            $j++;
            $log->debug("Processing function $pkg\::$f ...");
            $progress->update(pos => $i + $j/$numf, message => "Processing function $pkg\::$f ...") if $progress;
            $res = $pa->request(meta => "$uri$e");
            die "Can't meta $e: $res->[0] - $res->[1]" unless $res->[0] == 200;
            $cleanser->clean_in_place(my $meta = $res->[2]);
            $dbh->do("INSERT INTO function (package, name, summary, metadata) VALUES (?,?,?,?)", {}, $pkg, $f, $meta->{summary}, $json->encode($meta));
        }
    }
    $progress->finish if $progress;

    if ($args{delete} // 1) {
        my @deleted_pkgs;
        my $sth = $dbh->prepare("SELECT name FROM package");
        $sth->execute;
        while (my $row = $sth->fetchrow_hashref) {
            next if $row->{name} ~~ @pkgs;
            $log->info("Package $row->{name} no longer exists, deleting from database ...");
            push @deleted_pkgs, $row->{name};
        }
        if (@deleted_pkgs && !$args{-dry_run}) {
            my $in = join(",", map {$dbh->quote($_)} @deleted_pkgs);
            $dbh->do("DELETE FROM function WHERE package IN ($in)");
            $dbh->do("DELETE FROM package WHERE name IN ($in)");
        }
    }

    [200, "OK"];
}

1;
# ABSTRACT:
