aclocal
libtoolize --force
autoheader
automake --add-missing --copy --foreign
autoconf

find . -maxdepth 1 -type l | while read symlink; 
do 
	target=`readlink $symlink`
	rm -f $symlink
	cp $target $symlink
done
