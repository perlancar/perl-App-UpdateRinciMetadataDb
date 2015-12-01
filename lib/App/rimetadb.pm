package App::rimetadb;

# DATE
# VERSION

use 5.010001;
use strict;
use warnings;
use experimental 'smartmatch';
use Log::Any::IfLOG '$log';

use Module::Load qw(autoload load);

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

sub _cleanser {
    require Data::Clean::JSON;
    state $cleanser = Data::Clean::JSON->get_cleanser;
    $cleanser;
}

sub _json {
    require JSON;
    state $json = JSON->new->allow_nonref;
    $json;
}

sub _pa {
    require Perinci::Access::Perl;
    state $pa = Perinci::Access::Perl->new;
    $pa;
}

sub _connect_db {
    require DBI;
    require SQL::Schema::Versioned;

    my %args = @_;

    my $dbh = DBI->connect($args{dsn}, $args{user}, $args{password},
                           {RaiseError=>1});

    my $res = SQL::Schema::Versioned::create_or_update_db_schema(
        spec => $db_schema_spec,
        dbh => $dbh,
    );
    return $res unless $res->[0] == 200;
    ($res, $dbh);
}

our %SPEC;

$SPEC{':package'} = {
    v => 1.1,
    summary => 'Manage a Rinci metadata database',
};

our %args_common = (
    dsn => {
        summary => 'DBI connection DSN',
        description => <<'_',

Note: has been tested with MySQL and SQLite only.

_
        schema => 'str*',
        req => 1,
        tags => ['common'],
    },
    user => {
        summary => 'DBI connection user',
        schema => 'str*',
        tags => ['common'],
    },
    password => {
        summary => 'DBI connection password',
        schema => 'str*',
        tags => ['common'],
    },
);

our %args_query = (
    query => {
        schema => 'str*',
        pos => 0,
    },
    detail => {
        schema => 'bool',
        cmdline_aliases => {l=>{}},
    },
);

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

$SPEC{update_from_modules} = {
    v => 1.1,
    summary => 'Update Rinci metadata database from local Perl modules',
    description => <<'_',

This routine scans Perl modules, load them, and update the database using Rinci
metadata from each modules into the database.

_
    args => {
        %args_common,
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
            pos => 0,
            greedy => 1,
            'x.schema.element_entity' => 'modulename',
        },
        exclude => {
            summary => 'Perl package names or prefixes to exclude',
            schema => ['array*' => of => 'str*'],
            'x.schema.element_entity' => 'modulename',
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
            'x.schema.element_entity' => 'modulename',
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
            'x.schema.element_entity' => 'modulename',
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
            'x.schema.element_entity' => 'modulename',
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
sub update_from_modules {
    require Module::List;
    require Module::Path::More;
    require Package::MoreUtil;

    my %args = @_;

    my ($res, $dbh) = _connect_db(%args);
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

        $res = _pa->request(meta => "$uri");
        die "Can't meta $uri: $res->[0] - $res->[1]" unless $res->[0] == 200;
        _cleanser->clean_in_place(my $pkgmeta = $res->[2]);

        $res = _pa->request(list => $uri, {type=>'function'});
        die "Can't list $uri: $res->[0] - $res->[1]" unless $res->[0] == 200;
        my $numf = @{ $res->[2] };

        $dbh->do("INSERT INTO package (name, summary, metadata, mtime) VALUES (?,?,?,0)", {}, $pkg, $pkgmeta->{summary}, _json->encode($pkgmeta), $st[9]) unless $rec;
        $dbh->do("UPDATE package set mtime=? WHERE name=?", {}, $st[9], $pkg);
        $dbh->do("DELETE FROM function WHERE package=?", {}, $pkg);
        my $j = 0;
        for my $e (@{ $res->[2] }) {
            my $f = $e; $f =~ s!.+/!!;
            $j++;
            $log->debug("Processing function $pkg\::$f ...");
            $progress->update(pos => $i + $j/$numf, message => "Processing function $pkg\::$f ...") if $progress;
            $res = _pa->request(meta => "$uri$e");
            die "Can't meta $e: $res->[0] - $res->[1]" unless $res->[0] == 200;
            _cleanser->clean_in_place(my $meta = $res->[2]);
            $dbh->do("INSERT INTO function (package, name, summary, metadata) VALUES (?,?,?,?)", {}, $pkg, $f, $meta->{summary}, _json->encode($meta));
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

$SPEC{update} = {
    v => 1.1,
    summary => 'Add/update a package or function metadata',
    args => {
        %args_common,
        package => {
            schema => 'str*',
            req => 1,
        },
        function => {
            schema => 'str*',
        },
        metadata => {
            schema => 'hash*',
            req => 1,
        },
        dist => {
            schema => 'str*',
        },
        extra => {
            schema => 'str*',
        },
    },
};
sub update {
    require Perinci::Sub::Normalize;

    my %args = @_;

    my ($res, $dbh) = _connect_db(%args);
    return $res unless $res->[0] == 200;

    my $pkg  = $args{package};
    my $func = $args{function};
    my $meta = $args{metadata};

    if ($func) {
        $meta = Perinci::Sub::Normalize::normalize_function_metadata($meta);
    }

    my $pkgsummary;
    $pkgsummary = $meta->{summary} unless $func;
    if ($dbh->selectrow_array("SELECT name FROM package WHERE name=?", {}, $pkg)) {
        $dbh->do("UPDATE package (summary, metadata, mtime, dist, extra) VALUES (?,?,?,?,?) WHERE package=?",
                 {}, $pkgsummary, _json->encode($meta), time(), $args{dist}, $args{extra},
                 $pkg);
    } else {
        $dbh->do("INSERT INTO package (name, summary, metadata, mtime, extra) VALUES (?,?,?,?,?)",
                 {}, $pkg, $pkgsummary, _json->encode($meta), $args{dist}, $args{extra});
    }

    if ($func) {
        my $funcsummary = $meta->{summary};
        if ($dbh->selectrow_array("SELECT name FROM function WHERE package=? AND name=?", {}, $pkg, $func)) {
            $dbh->do("UPDATE function (summary, metadata, mtime, extra) VALUES (?,?,?,?) WHERE package=? AND function=?",
                     {}, $funcsummary, _json->encode($meta), time(), $args{extra},
                     $pkg, $func);
        } else {
            $dbh->do("INSERT INTO package (package, name, summary, metadata, mtime, extra) VALUES (?,?,?,?,?,?)",
                     {}, $pkg, $func, $funcsummary, _json->encode($meta), time(), $args{extra});
        }
    }

    [200, "OK"];
}

$SPEC{delete} = {
    v => 1.1,
    summary => 'Delete a package or function metadata',
    args => {
        %args_common,
        package => {
            schema => 'str*',
            req => 1,
        },
        function => {
            schema => 'str*',
        },
    },
};
sub delete {
    my %args = @_;

    my ($res, $dbh) = _connect_db(%args);
    return $res unless $res->[0] == 200;

    my $pkg  = $args{package};
    my $func = $args{function};

    if ($func) {
        $dbh->do("DELETE FROM function WHERE package=? AND name=?", {}, $pkg, $func);
    } else {
        $dbh->do("DELETE FROM function WHERE package=?", {}, $pkg);
        $dbh->do("DELETE FROM package WHERE name=?", {}, $pkg);
    }
    [200, "OK"];
}

$SPEC{packages} = {
    v => 1.1,
    summary => 'List packages',
    args => {
        %args_common,
        %args_query,
    },
};
sub packages {
    my %args = @_;

    my ($res, $dbh) = _connect_db(%args);
    return $res unless $res->[0] == 200;

    my $q  = $args{query};

    my @rows;
    my @columns = qw(name summary dist mtime extra);
    my @wheres;
    my @binds;

    if (length $q) {
        push @wheres, "(name LIKE ? OR dist LIKE ? OR extra LIKE ?)";
        push @binds, $q, $q, $q;
    }

    my $sth = $dbh->prepare(
        "SELECT name,summary,dist,mtime,extra FROM package ORDER by name".
            (@wheres ? " WHERE ".join(" AND ", @wheres) : "")
    );
    $sth->execute(@binds);

    while (my $row = $sth->fetchrow_hashref) {
        if ($args{detail}) {
            push @rows, $row;
        } else {
            push @rows, $row->{name};
        }
    }

    [200, "OK", \@rows, {'table.fields'=>\@columns}];
}

$SPEC{functions} = {
    v => 1.1,
    summary => 'List functions',
    args => {
        %args_common,
        %args_query,
    },
};
sub functions {
    my %args = @_;

    my ($res, $dbh) = _connect_db(%args);
    return $res unless $res->[0] == 200;

    my $q  = $args{query};

    my @rows;
    my @columns = qw(package name summary extra);
    my @wheres;
    my @binds;

    if (length $q) {
        push @wheres, "(package LIKE ? OR name LIKE ? OR extra LIKE ?)";
        push @binds, $q, $q, $q;
    }

    my $sth = $dbh->prepare(
        "SELECT package,name,summary,extra FROM function ORDER by package,name".
            (@wheres ? " WHERE ".join(" AND ", @wheres) : "")
    );
    $sth->execute(@binds);

    while (my $row = $sth->fetchrow_hashref) {
        if ($args{detail}) {
            push @rows, $row;
        } else {
            push @rows, $row->{name};
        }
    }

    [200, "OK", \@rows, {'table.fields'=>\@columns}];
}

$SPEC{stats} = {
    v => 1.1,
    summary => 'Show some statistics',
};
sub stats {
    my %args = @_;

    my ($res, $dbh) = _connect_db(%args);
    return $res unless $res->[0] == 200;

    my %stats;
    ($stats{num_packages}) = $dbh->selectrow_array("SELECT COUNT(*) FROM package");
    ($stats{num_functions}) = $dbh->selectrow_array("SELECT COUNT(*) FROM function");

    # XXX avg_num_args
    # XXX top_arg_types
    # XXX top_arg_entities
    # XXX pct_arg_has_entity
    # XXX pct_arg_has_element_entity

    [200, "OK", \%stats];
}

1;
# ABSTRACT:

=head1 SYNOPSIS

See the included CLI script L<rimetadb>.


=head1 SEE ALSO

L<Rinci>