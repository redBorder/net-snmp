#
# 5.4+ enables Perl by default
#
%define netsnmp_embedded_perl 1
%define netsnmp_perl_modules 1
%define netsnmp_cflags ""
%define netsnmp_epoch 2

# ugly RHEL detector
# SuSE build service defines rhel_version, RHEL itself defines nothing
%if 0%{?rhel_version}
%define rhel %{?rhel_version}
%else
%define is_rhel %(grep -E "Red Hat Enterprise Linux|CentOS" /etc/redhat-release &>/dev/null && echo 1 || echo 0)
%if %{is_rhel}
%define rhel %(sed </etc/redhat-release -e 's/.*release \\(.\\).*/\\1/'  )
%endif
%endif

# because perl(Tk) is optional, automatic dependencies will never succeed:
%define _use_internal_dependency_generator 0
%define __find_requires %{_builddir}/net-snmp-%{version}/dist/find-requires
%define __find_provides /usr/lib/rpm/find-provides

#
# Check for -without embedded_perl
#
%{?_without_embedded_perl:%undefine netsnmp_embedded_perl}
#
# check for -without perl_modules
#
%{?_without_perl_modules:%undefine netsnmp_perl_modules}
#
# if embedded_perl or perl_modules specified, include some Perl stuff
#
%if 0%{?netsnmp_embedded_perl} || 0%{?netsnmp_perl_modules}
%define netsnmp_include_perl 1
%endif
Summary: Tools and services for the SNMP protocol
Name: net-snmp
Version: 5.9.5.2.2
Release: 7
Epoch: %{netsnmp_epoch}
URL: http://www.net-snmp.org/
License: BSDish
Group: System Environment/Daemons
Vendor: Net-SNMP project
Source: net-snmp-%{version}.tar.gz
Obsoletes: cmu-snmp ucd-snmp ucd-snmp-utils
Obsoletes: net-snmp-libs < %{netsnmp_epoch}:%{version}-%{release}
Obsoletes: net-snmp-utils < %{netsnmp_epoch}:%{version}-%{release}
Provides: net-snmp = %{netsnmp_epoch}:%{version}-%{release}
Provides: net-snmp-libs = %{netsnmp_epoch}:%{version}-%{release}
Provides: net-snmp-utils = %{netsnmp_epoch}:%{version}-%{release}
BuildRoot: /tmp/%{name}-root
Packager: The Net-SNMP Coders <http://sourceforge.net/projects/net-snmp/>
Requires: openssl, popt, rpm, zlib, bzip2-libs, glibc
# Explicitly provide libraries so redborder-monitor finds them
Provides: libnetsnmp.so.40()(64bit)
Provides: libnetsnmpagent.so.40()(64bit)
Provides: libnetsnmphelpers.so.40()(64bit)
Provides: libnetsnmpmibs.so.40()(64bit)
Provides: libnetsnmptrapd.so.40()(64bit)

# Net-SNMP requires either openssl-devel or libressl-devel at build time, but
# how to express this in an RPM spec file?
BuildRequires: autoconf
BuildRequires: automake
BuildRequires: bzip2
BuildRequires: gcc
BuildRequires: perl
BuildRequires: rpm-devel
BuildRequires: libnl3-devel
BuildRequires: librdkafka-devel
%if 0%{?netsnmp_embedded_perl}
Requires: perl
BuildRequires: perl(ExtUtils::Embed)
%endif

%if 0%{?fedora}%{?rhel}
# Fedora & RHEL specific requires/provides
Provides: net-snmp-agent-libs = %{netsnmp_epoch}:%{version}-%{release}

# RHEL or Fedora
%if 0%{?fedora} >= 9
Provides: net-snmp-gui
Obsoletes: net-snmp-gui
# newer fedoras need following macro to compile with new rpm
%define netsnmp_cflags "-D_RPM_4_4_COMPAT"
%else
BuildRequires: beecrypt-devel
%endif
%endif

%description

Net-SNMP provides tools and libraries relating to the Simple Network
Management Protocol including: An extensible agent, An SNMP library,
tools to request or set information from SNMP agents, tools to
generate and handle SNMP traps, etc.  Using SNMP you can check the
status of a network of computers, routers, switches, servers, ... to
evaluate the state of your network.

%if 0%{?netsnmp_embedded_perl}
This package includes embedded Perl support within the agent.
%endif

%package devel
Group: Development/Libraries
Summary: The includes and static libraries from the Net-SNMP package.
AutoReqProv: no
Requires: net-snmp = %{netsnmp_epoch}:%{version}-%{release}
Obsoletes: cmu-snmp-devel ucd-snmp-devel

%description devel
The net-snmp-devel package contains headers and libraries which are
useful for building SNMP applications, agents, and sub-agents.

%if 0%{?netsnmp_include_perl}
%package perlmods
Group: System Environment/Libraries
Summary: The Perl modules provided with Net-SNMP
AutoReqProv: no
Requires: net-snmp = %{netsnmp_epoch}:%{version}-%{release}, perl

%if 0%{?fedora}%{?rhel}
Provides: net-snmp-perl
Provides: perl(SNMP) perl(NetSNMP::OID)
Provides: perl(NetSNMP::ASN)
Provides: perl(NetSNMP::AnyData::Format::SNMP) perl(NetSNMP::AnyData::Storage::SNMP)
Provides: perl(NetSNMP::agent)
Provides: perl(NetSNMP::manager) perl(NetSNMP::TrapReceiver)
Provides: perl(NetSNMP::default_store) perl(NetSNMP::agent::default_store)
Obsoletes: net-snmp-perl
%endif

%description perlmods
Net-SNMP provides a number of Perl modules useful when using the SNMP
protocol.  Both client and agent support modules are provided.
%endif

%prep
%if 0%{?netsnmp_embedded_perl} != 0 && 0%{?netsnmp_perl_modules} == 0
echo "'-with embedded_perl' requires '-with perl_modules'"
exit 1
%endif

%setup -q

%build
# Clear Perl local configuration to avoid installation in home directories
unset PERL_MM_OPT
unset PERL_MB_OPT
unset PERL5LIB

options=()
options+=(--enable-shared)
options+=(--sysconfdir="/etc/net-snmp")
options+=(--libdir=%{_libdir})
options+=(--with-cflags="$RPM_OPT_FLAGS %{netsnmp_cflags}")
options+=(--with-defaults)
options+=(--with-mib-modules="smux")
options+=(--with-sys-contact="Unknown")
options+=(--with-rdkafka)
%if 0%{?netsnmp_perl_modules}
options+=(--with-perl-modules="INSTALLDIRS=vendor")
%else
options+=(--without-perl-modules)
%endif
%if 0%{?netsnmp_embedded_perl}
options+=(--enable-embedded-perl)
%else
options+=(--disable-embedded-perl)
%endif

%configure "${options[@]}"

make

%install
# Clear Perl local configuration to avoid installation in home directories
unset PERL_MM_OPT
unset PERL_MB_OPT
unset PERL5LIB

# ----------------------------------------------------------------------
# 'install' sets the current directory to _topdir/BUILD/{name}-{version}
# ----------------------------------------------------------------------
rm -rf $RPM_BUILD_ROOT

make DESTDIR=%{buildroot} install

# Remove 'snmpinform' from the temporary directory because it is a
# symbolic link, which cannot be handled by the rpm installation process.
%__rm -f $RPM_BUILD_ROOT%{_prefix}/bin/snmpinform
# install the init script
mkdir -p $RPM_BUILD_ROOT/etc/rc.d/init.d
perl -i -p -e 's@/usr/local/share/snmp/@/etc/snmp/@g;s@usr/local@%{_prefix}@g' dist/snmpd-init.d
install -m 755 dist/snmpd-init.d $RPM_BUILD_ROOT/etc/rc.d/init.d/snmpd

%if 0%{?netsnmp_include_perl}
# unneeded Perl stuff
find $RPM_BUILD_ROOT -name Bundle -type d | xargs rm -rf
find $RPM_BUILD_ROOT -name perllocal.pod | xargs rm -f

# store a copy of installed Perl stuff.  It's too complex to predict
(xxdir=`pwd` && cd $RPM_BUILD_ROOT && find . -path "*/perl5/*" -type f | sed 's/^\.//' > $xxdir/net-snmp-perl-files)
%endif

%post -p /sbin/ldconfig
# Create the symbolic link 'snmpinform' after all other files have
# been installed.
%__rm -f %{_bindir}/snmpinform
%__ln_s %{_bindir}/snmptrap %{_bindir}/snmpinform

%preun
# ----------------------------------------------------------------------
# The 'preun' script is executed just before the package is erased.
# ----------------------------------------------------------------------
# Remove the symbolic link 'snmpinform' before anything else, in case
# it is in a directory that rpm wants to remove (at present, it isn't).
%__rm -f %{_bindir}/snmpinform

%postun -p /sbin/ldconfig

%clean
rm -rf $RPM_BUILD_ROOT

%files
%defattr(-,root,root)

# Install the following documentation in _defaultdocdir/{name}-{version}/
%doc AGENT.txt ChangeLog CodingStyle COPYING
%doc EXAMPLE.conf.def FAQ INSTALL NEWS PORTING TODO
%doc README README.agentx README.hpux11 README.krb5
%doc README.snmpv3 README.solaris README.thread README.win32
%doc README.aix README.osX README.tru64 README.irix README.agent-mibs
%doc README.Panasonic_AM3X.txt

# % config(noreplace) /etc/net-snmp/snmpd.conf
	 
# % {_datadir}/snmp/snmpconf-data
%{_datadir}/snmp

%{_bindir}/*
%{_sbindir}/*
%{_mandir}/man1/*
# don't include Perl man pages, which start with caps
%{_mandir}/man3/[^A-Z]*
%{_mandir}/man5/*
%{_mandir}/man8/*
%{_libdir}/*.so*
%{_libdir}/pkgconfig/*.pc
/etc/rc.d/init.d/snmpd

%files devel
%defattr(-,root,root)

%{_includedir}/*
%{_libdir}/*.a
%{_libdir}/pkgconfig/*.pc
%{_libdir}/*.la

%if 0%{?netsnmp_include_perl}
%files -f net-snmp-perl-files perlmods
%defattr(-,root,root)
%{_mandir}/man3/NetSNMP*
%{_mandir}/man3/SNMP*
%endif

%changelog
* Fri Mar 06 2026 Jose Jimenez <jjimenez@redborder.com> - 5.9.5.2.2-7
- Fixed epoch macro and added versioned obsoletes
