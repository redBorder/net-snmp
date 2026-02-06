#
# 5.4+ enables Perl by default
#
%define netsnmp_embedded_perl 1
%define netsnmp_perl_modules 1
%define netsnmp_cflags ""

%define _prefix /opt/rb-net-snmp
%define _exec_prefix %{_prefix}
%define _bindir %{_exec_prefix}/bin
%define _sbindir %{_exec_prefix}/sbin
%define _libdir %{_exec_prefix}/lib
%define _includedir %{_prefix}/include
%define _datadir %{_prefix}/share
%define _mandir %{_prefix}/share/man
%define _infodir %{_prefix}/share/info
%define _sysconfdir %{_prefix}/etc

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
%define __find_requires %{_builddir}/rb-net-snmp-%{version}/dist/find-requires
%define __find_provides /usr/lib/rpm/find-provides

%define _rb_netsnmp_prefix /opt/rb-net-snmp

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
Name: rb-net-snmp
Version: 5.9.5.2.2
# update release for vendor release. (eg 1.fc6, 1.rh72, 1.ydl3, 1.ydl23)
Release: 1
URL: http://www.net-snmp.org/
License: BSDish
Group: System Environment/Daemons
Vendor: Net-SNMP project
Source: rb-net-snmp-%{version}.tar.gz
Source1: snmptrapd_kafka.c
BuildRoot: /tmp/%{name}-root
Packager: The Net-SNMP Coders <http://sourceforge.net/projects/net-snmp/>
Requires: openssl, popt, rpm, zlib, bzip2-libs, glibc
# Net-SNMP requires either openssl-devel or libressl-devel at build time, but
# how to express this in an RPM spec file?
BuildRequires: autoconf
BuildRequires: automake
BuildRequires: bzip2
BuildRequires: gcc
BuildRequires: perl
BuildRequires: rpm-devel
%if 0%{?netsnmp_embedded_perl}
Requires: perl
BuildRequires: perl(ExtUtils::Embed)
%endif

%if 0%{?fedora}%{?rhel}
# Fedora & RHEL specific requires/provides
Epoch: 2

# RHEL or Fedora
%if 0%{?fedora} >= 9
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
Requires: rb-net-snmp = %{?epoch:%{epoch}:}%{version}-%{release}

%description devel
The net-snmp-devel package contains headers and libraries which are
useful for building SNMP applications, agents, and sub-agents.

%if 0%{?netsnmp_include_perl}
%package perlmods
Group: System Environment/Libraries
Summary: The Perl modules provided with Net-SNMP
AutoReqProv: no
Requires: rb-net-snmp = %{?epoch:%{epoch}:}%{version}-%{release}, perl

%if 0%{?fedora}%{?rhel}
Provides: perl(SNMP) perl(NetSNMP::OID)
Provides: perl(NetSNMP::ASN)
Provides: perl(NetSNMP::AnyData::Format::SNMP) perl(NetSNMP::AnyData::Storage::SNMP)
Provides: perl(NetSNMP::agent)
Provides: perl(NetSNMP::manager) perl(NetSNMP::TrapReceiver)
Provides: perl(NetSNMP::default_store) perl(NetSNMP::agent::default_store)
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
cp %{SOURCE1} apps/

%build
unset PERL5LIB
unset PERL_LOCAL_LIB_ROOT
unset PERL_MB_INSTALL_BASE
unset PERL_MM_OPT
options=()
options+=(--prefix=%{_rb_netsnmp_prefix})
options+=(--enable-shared)
options+=(--sysconfdir="%{_rb_netsnmp_prefix}/etc/net-snmp")
options+=(--with-cflags="$RPM_OPT_FLAGS %{netsnmp_cflags}")
options+=(--with-defaults)
options+=(--with-mib-modules="smux")
options+=(--with-sys-contact="Unknown")
options+=(--with-rdkafka)
%if 0%{?netsnmp_perl_modules}
options+=(--with-perl-modules)
%else
options+=(--without-perl-modules)
%endif
options+=(--with-perl-options="INSTALL_BASE=/opt/rb-net-snmp INSTALLDIRS=vendor INSTALLMAN3DIR=%{_rb_netsnmp_prefix}/share/man/man3")
%if 0%{?netsnmp_embedded_perl}
options+=(--enable-embedded-perl)
%else
options+=(--disable-embedded-perl)
%endif

%configure "${options[@]}"

make

%install
# ----------------------------------------------------------------------
# 'install' sets the current directory to _topdir/BUILD/{name}-{version}
# ----------------------------------------------------------------------
rm -rf $RPM_BUILD_ROOT

make DESTDIR=%{buildroot} install prefix=%{_rb_netsnmp_prefix}

%if 0%{?netsnmp_include_perl}
if [ -d %{buildroot}/usr/local/share/man/man3 ]; then
  mkdir -p %{buildroot}%{_mandir}/man3
  mv %{buildroot}/usr/local/share/man/man3/* %{buildroot}%{_mandir}/man3/
fi
%endif

# Create config directory
mkdir -p %{buildroot}%{_sysconfdir}/net-snmp/snmp
# Create a dummy snmptrapd.conf to ensure the directory is installed
tee %{buildroot}%{_sysconfdir}/net-snmp/snmp/snmptrapd.conf <<'EOF'
disableAuthorization yes
kafkaBrokers kafka.service:9092
kafkaTopic rb_trap
EOF

# Create ld.so.conf.d entry
mkdir -p %{buildroot}/etc/ld.so.conf.d
echo "%{_rb_netsnmp_prefix}/lib" > %{buildroot}/etc/ld.so.conf.d/rb-net-snmp.conf

# Remove 'snmpinform' from the temporary directory because it is a
# symbolic link, which cannot be handled by the rpm installation process.
%__rm -f %{buildroot}%{_rb_netsnmp_prefix}/bin/snmpinform

# Install systemd service
mkdir -p %{buildroot}%{_unitdir}
install -m 644 dist/snmpd.service %{buildroot}%{_unitdir}/rb-snmpd.service
# Update binary path in service file
sed -i 's|/usr/sbin/snmpd|%{_rb_netsnmp_prefix}/sbin/rb-snmpd|g' %{buildroot}%{_unitdir}/rb-snmpd.service

# Rename binaries
mv %{buildroot}%{_rb_netsnmp_prefix}/sbin/snmpd %{buildroot}%{_rb_netsnmp_prefix}/sbin/rb-snmpd
mv %{buildroot}%{_rb_netsnmp_prefix}/sbin/snmptrapd %{buildroot}%{_rb_netsnmp_prefix}/sbin/rb-snmptrapd
for i in agentxtrap net-snmp-create-v3-user snmpconf encode_keychange snmpbulkget snmpbulkwalk snmpdelta snmpdf snmpget snmpgetnext snmpnetstat snmpping snmpset snmpstatus snmptable snmptest snmptranslate snmptrap snmpusm snmpvacm snmpwalk; do
    mv %{buildroot}%{_rb_netsnmp_prefix}/bin/$i %{buildroot}%{_rb_netsnmp_prefix}/bin/rb-$i
done

%if 0%{?netsnmp_include_perl}
# unneeded Perl stuff
find %{buildroot}%{_rb_netsnmp_prefix}/lib*/perl5/ -name Bundle -type d | xargs rm -rf
find %{buildroot}%{_rb_netsnmp_prefix}/lib*/perl5/ -name perllocal.pod | xargs rm -f

# store a copy of installed Perl stuff.  It's too complex to predict
(xxdir=`pwd` && cd %{buildroot} && ls -lR > $xxdir/buildroot-listing.txt && find . -path '*/perl*/*' -type f | sed -e 's,^\\./,,' -e 's,^,/,g' > $xxdir/net-snmp-perl-files)


%endif

%post
# ----------------------------------------------------------------------
# The 'post' script is executed just after the package is installed.
# ----------------------------------------------------------------------
# Create the symbolic link 'snmpinform' after all other files have
# been installed.
%__rm -f %{_rb_netsnmp_prefix}/bin/snmpinform
%__ln_s %{_rb_netsnmp_prefix}/bin/rb-snmptrap %{_rb_netsnmp_prefix}/bin/snmpinform

# run ldconfig
/sbin/ldconfig

%preun
# ----------------------------------------------------------------------
# The 'preun' script is executed just before the package is erased.
# ----------------------------------------------------------------------
# Remove ld.so.conf.d entry
rm -f /etc/ld.so.conf.d/rb-net-snmp.conf

# Remove the symbolic link 'snmpinform' before anything else, in case
# it is in a directory that rpm wants to remove (at present, it isn't).
%__rm -f %{_rb_netsnmp_prefix}/bin/snmpinform

%postun
# ----------------------------------------------------------------------
# The 'postun' script is executed just after the package is erased.
/sbin/ldconfig

%clean
rm -rf $RPM_BUILD_ROOT

%files
%defattr(-,root,root)
%config /etc/ld.so.conf.d/rb-net-snmp.conf

# Install the following documentation in _defaultdocdir/{name}-{version}/
%doc AGENT.txt ChangeLog CodingStyle COPYING
%doc EXAMPLE.conf.def FAQ INSTALL NEWS PORTING TODO
%doc README README.agentx README.hpux11 README.krb5
%doc README.snmpv3 README.solaris README.thread README.win32
%doc README.aix README.osX README.tru64 README.irix README.agent-mibs
%doc README.Panasonic_AM3X.txt

# % config(noreplace) /etc/net-snmp/snmpd.conf
	 
# % {_datadir}/snmp/snmpconf-data
%{_rb_netsnmp_prefix}/share/snmp
%config(noreplace) %{_sysconfdir}/net-snmp/snmp/snmptrapd.conf

%{_rb_netsnmp_prefix}/bin/*
%{_rb_netsnmp_prefix}/sbin/*
%{_rb_netsnmp_prefix}/share/man/man1/*
# don't include Perl man pages, which start with caps
%{_rb_netsnmp_prefix}/share/man/man3/[^A-Z]*
%{_rb_netsnmp_prefix}/share/man/man5/*
%{_rb_netsnmp_prefix}/share/man/man8/*
%{_rb_netsnmp_prefix}/lib*/*.so*
%{_rb_netsnmp_prefix}/lib*/pkgconfig/*.pc
%{_unitdir}/rb-snmpd.service

%files devel
%defattr(-,root,root)

%{_rb_netsnmp_prefix}/include
%{_rb_netsnmp_prefix}/lib*/*.a
%{_rb_netsnmp_prefix}/lib*/*.la

%if 0%{?netsnmp_include_perl}
%files -f net-snmp-perl-files perlmods
%defattr(-,root,root)
%{_rb_netsnmp_prefix}/share/man/man3/*
%endif

%changelog
* Sat Aug 22 2020 Bart Van Assche <bvanassche@acm.org>
- Fixed the warnings reported by rpmbuild about this spec file.

* Sat Dec 15 2012 Magnus Fromreide <magfr@lysator.liu.se>
- Make the -without options to rpmbuild work

* Thu Jul 26 2012 Dave Shield <D.T.Shield@liverpool.ac.uk>
- Additional "Provides:" to complete the list of perl modules
  Triggered by Bug ID #3540621

* Thu Oct  7 2010 Peter Green <peter.green@az-tek.co.uk>
- Modified RHEL detection to include CentOS.
- Added extra "Provides:" to the perlmods package definition;
  otherwise subsequent package installations that require certain
  Perl modules try to re-install RHEL/CentOS stock net-snmp

* Tue May  6 2008 Jan Safranek <jsafranek@users.sf.net>
- remove %{libcurrent}
- add openssl-devel to build requirements
- don't use Provides: unless necessary, let rpmbuild compute the provided 
  libraries

* Tue Jun 19 2007 Thomas Anders <tanders@users.sf.net>
- add "BuildRequires: perl-ExtUtils-Embed", e.g. for Fedora 7

* Wed Nov 22 2006 Thomas Anders <tanders@users.sf.net>
- fixes for 5.4 and 64-bit platforms
- enable Perl by default, but allow for --without perl_modules|embedded_perl
- add netsnmp_ prefix for local defines

* Fri Sep  1 2006 Thomas Anders <tanders@users.sf.net>
- Update to 5.4.dev
- introduce %{libcurrent}
- use new disman/event name
- add: README.aix README.osX README.tru64 README.irix README.agent-mibs
  README.Panasonic_AM3X.txt
- add new NetSNMP::agent::Support

* Fri Jan 13 2006 hardaker <hardaker@users.sf.net>
- Update to 5.3.0.1

* Wed Dec 28 2005 hardaker <hardaker@users.sf.net>
- Update to 5.3

* Tue Oct 28 2003 rs <rstory@users.sourceforge.net>
- fix conditional perl build after reading rpm docs

* Sat Oct  4 2003 rs <rstory@users.sourceforge.net> - 5.0.9-4
- fix to build without requiring arguments
- separate embedded perl and perl modules options
- fix fix for init.d script for non-/usr/local installation

* Fri Sep 26 2003 Wes Hardaker <hardaker@users.sourceforge.net>
- fix perl's UseNumeric
- fix init.d script for non-/usr/local installation

* Fri Sep 12 2003 Wes Hardaker <hardaker@users.sourceforge.net>
- fixes for 5.0.9's perl support

* Mon Sep 01 2003 Wes Hardaker <hardaker@users.sourceforge.net>
- added perl support

* Wed Oct 09 2002 Wes Hardaker <hardaker@users.sourceforge.net>
- Incorperated most of Mark Harig's better version of the rpm spec and Makefile

* Wed Oct 09 2002 Wes Hardaker <hardaker@users.sourceforge.net>
- Made it possibly almost usable.

* Mon Apr 22 2002 Robert Story <rstory@users.sourceforge.net>
- created