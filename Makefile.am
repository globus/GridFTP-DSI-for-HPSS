SUBDIRS=source test

package:: rpm

rpmmacros::
	@echo "%_topdir  $(PWD)/rpmbuild" > ~/.rpmmacros
	@echo "%_tmppath $(PWD)/rpmbuild/tmp" >> ~/.rpmmacros

rpmbuild_dirs:: 
	@rm -rf rpmbuild
	@mkdir rpmbuild
	@mkdir rpmbuild/BUILD
	@mkdir rpmbuild/RPMS
	@mkdir rpmbuild/SOURCES
	@mkdir rpmbuild/SPECS
	@mkdir rpmbuild/SRPMS
	@mkdir rpmbuild/tmp

rpm:: dist rpmmacros rpmbuild_dirs
	@cp @PACKAGE_NAME@*.tar.gz rpmbuild/SOURCES/
	@cp packaging/fedora/@PACKAGE_NAME@.spec rpmbuild/SPECS
	@rpmbuild -bb --nodeps ./rpmbuild/SPECS/globus-gridftp-server-hpss.spec
	@rpmbuild -bb --nodeps ./rpmbuild/SPECS/globus-gridftp-server-hpss.spec
	@cp rpmbuild/RPMS/x86_64/* .

tarball:
	rm -rf $(abs_top_builddir)/staging
	$(MAKE) install DESTDIR=$(abs_top_builddir)/staging prefix=/usr \
		libdir=$$([ -f /etc/debian_version ] && echo /usr/lib/x86_64-linux-gnu || echo /usr/lib64)
	find $(abs_top_builddir)/staging -name 'lib*.la' -delete
	. /etc/os-release && \
	tar czf $(PACKAGE)-$(VERSION).$$ID$$VERSION_ID.tar.gz -C $(abs_top_builddir)/staging .
	rm -rf $(abs_top_builddir)/staging

EXTRA_DIST=data
