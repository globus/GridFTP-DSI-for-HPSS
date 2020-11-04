#SUBDIRS = unit
#DIST_SUBDIRS = unit integration utils
#
## including framework/ as EXTRA_DIST, because we don't build it directly
## so including framework/ in DIST_SUBDIRS breaks source packaging.
#EXTRA_DIST = framework

## XXX EXTRA_DIST == DIST_SUBDIRS except subdirectories of EXTRA_DIST
## are not expected to contain Makefiles that enumerate which sources
## need to be distributed

SUBDIRS = framework unit
DIST_SUBDIRS = $(SUBDIRS) integration utils
