test : test.c
	libtool --mode=link --tag=CC gcc /usr/local/lib/libzookeeper_mt.la test.c -o test

test_st : test.c
	libtool --mode=link --tag=CC gcc /usr/local/lib/libzookeeper_st.la test.c -o test_st

clean : 
	rm -f test test_mt test_st
