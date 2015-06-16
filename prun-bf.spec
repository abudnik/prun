Summary:    Distributed cron and job scheduler
Name:		prun
Version:	0.14
Release:	1%{?dist}.1

License:	Apache License, Version 2.0
Group:		Applications/System
URL:		https://github.com/abudnik/prun
Source0:	%{name}-%{version}.tar.bz2
BuildRoot:	%{_tmppath}/%{name}-%{version}-%{release}-root-%(%{__id_u} -n)

BuildRequires:	cmake
BuildRequires:	boost-devel

%description
Distributed cron and job scheduler


%package master
Summary:	Distributed cron and job scheduler (master)
Group:		Applications/System

%description master
Distributed cron and job scheduler (master)


%package worker
Summary:	Distributed cron and job scheduler (worker)
Group:		Applications/System

%description worker
Distributed cron and job scheduler (worker)


%package client
Summary:	Distributed cron and job scheduler (client)
Group:		Applications/System

%description client
Distributed cron and job scheduler (client)


%prep
%setup -q

%build
export DESTDIR="%{buildroot}"
%{cmake} -DRelease=True .

make %{?_smp_mflags}


%install
rm -rf %{buildroot}
make install DESTDIR=%{buildroot}
rm -f %{buildroot}%{_libdir}/*.a
rm -f %{buildroot}%{_libdir}/*.la
mv %{buildroot}%{_sysconfdir}/init.d/prun-worker.init %{buildroot}%{_sysconfdir}/init.d/prun-worker
mv %{buildroot}%{_sysconfdir}/init.d/prun-master.init %{buildroot}%{_sysconfdir}/init.d/prun-master

%post worker
if ! getent group prun >/dev/null; then
    groupadd --system prun
fi
if ! getent passwd prun-master >/dev/null; then
    adduser --system \
            --groups prun \
            --no-user-group \
            --home-dir /var/lib/pworker \
            --no-create-home \
            --shell /bin/false \
            --comment "prun worker" \
            prun-worker
fi
chown -R prun-worker:prun /etc/pworker /var/lib/pworker
chmod +x /etc/init.d/prun-worker
chkconfig --add prun-worker
chkconfig --level 345 prun-worker on
/etc/init.d/prun-worker start

%preun worker
/etc/init.d/prun-worker stop
chkconfig --del prun-worker

%post master
chmod +x /etc/init.d/prun-master
chkconfig --add prun-master
chkconfig --level 345 prun-master on
/etc/init.d/prun-master start

%preun master
/etc/init.d/prun-master stop
chkconfig --del prun-master

%clean
rm -rf %{buildroot}


%define _unpackaged_files_terminate_build 0

%files master
%defattr(-,root,root,-)
%{_bindir}/pmaster
%{_sysconfdir}/pmaster/master.cfg
%{_sysconfdir}/pmaster/groups
%{_sysconfdir}/pmaster/hosts_group1
%{_var}/lib/pmaster/*
%config %{_sysconfdir}/init.d/prun-master

%files worker
%defattr(-,root,root,-)
%{_bindir}/pworker
%{_bindir}/prexec
%{_sysconfdir}/pworker/worker.cfg
%{_var}/lib/pworker/*
%config %{_sysconfdir}/init.d/prun-worker

%files client
%defattr(-,root,root,-)
%{_bindir}/prun


%changelog
* Mon Jun 15 2015 Andrey Budnik <budnik27@gmail.com> - 0.14
- Initial release.
