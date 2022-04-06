JDKPATH = /usr
LIBPATH = ../../
CLASSPATH = $(JDKPATH)/lib/classes.zip:.:..:$(LIBPATH)
BINPATH = $(JDKPATH)/bin
JAVAC = $(JDKPATH)/bin/javac -classpath $(CLASSPATH)
JAVA  = $(JDKPATH)/bin/java  -classpath $(CLASSPATH)

PROGS = bp

all: $(PROGS)

bp:BasicPattern.java
	$(JAVAC) BasicPattern.java

clean:
	\rm -f *.class *~ \#* core