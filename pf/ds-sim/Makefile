CC=gcc
JCC=javac
CFLAGS=-g -Wall -O3 -fgnu89-inline

all: ds-server TestPF

ds-server: ds_server.c log_normal.c
	$(CC) $(CFLAGS) `xml2-config --cflags` $^ -o $@ `xml2-config --libs` -lm

TestPF: TestPF.java
	$(JCC) TestPF.java

clean:
	rm ds-server
	rm *.class
        
 
