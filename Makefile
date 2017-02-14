test : test.c
	libtool --mode=link --tag=CC gcc /usr/local/lib/libzookeeper_st.la test.c -o test
