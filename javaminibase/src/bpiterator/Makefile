JDKPATH = /usr
LIBPATH = ../../
CLASSPATH = $(JDKPATH)/lib/classes.zip:.:..:$(LIBPATH)
BINPATH = $(JDKPATH)/bin
JAVAC = $(JDKPATH)/bin/javac -classpath $(CLASSPATH)
JAVA  = $(JDKPATH)/bin/java  -classpath $(CLASSPATH)

PROGS = bpi

all: $(PROGS)

bpi:BPIterator.java
	$(JAVAC) BPIterator.java
clean:
	\rm -f *.class *~ \#* core 
