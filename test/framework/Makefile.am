noinst_LIBRARIES = libframework.a
libframework_a_SOURCES = \
	_display.c \
	_display.h \
	_events.c \
	_events.h \
	_expectations.c \
	_expectations.h \
	mocking.h \
	testing.c \
	testing.h

check_PROGRAMS = test_framework
TESTS = $(check_PROGRAMS)

test_framework_SOURCES = driver.c
test_framework_LDADD = libframework.a
