CXX=g++

all: clean compile

compile:
	${CXX} -O3 -pthread -std=c++11 MergeFeeds.cpp -o composite_top

clean:
	rm -rf SymbolLog/*
	rm -rf composite_top
	rm -rf TopOfBook.csv
