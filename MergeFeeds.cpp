#include <iostream>
#include <fstream>
#include <sstream>
#include <map>
#include <queue>
#include <unordered_map>
#include <unordered_set>
#include <cstdlib>
#include <utility>
#include "thread_safe_queue.h"
#include <thread>
#include <exception>

//#define DEBUG 0

using namespace std;
struct Feed {
  string Time;
  string Symbol;
  float BidPrice;
  unsigned BidSize;
  float AskPrice;
  unsigned AskSize;
};

// std::cout mutex to print exception messages.
mutex out_mutex;

//======================================================================
// Producer/Consumer logic to read files.
//======================================================================
void enqueue(char *FileName,
             ThreadsafeQueue<Feed> &FeedsQueue) {
  string Time = "";
  string Symbol = "";
  string BidPrice = "";
  string BidSize = "";
  string AskPrice = "";
  string AskSize = "";
  //string PrevTime = "";
  //string PrevSymbol = "";
  ifstream inf;

  try {
    inf.open(FileName);
  } catch (const exception &e) {
    lock_guard<mutex> out_lck(out_mutex);
    cout << "\nError opening input file : "
         << e.what();
  }

  string ignore;
  getline(inf, ignore);
  string line = "";

  while (getline(inf, line)) {
    stringstream infStream(line);
    //PrevTime = Time;
    //PrevSymbol = Symbol;
    getline(infStream, Time, ',');
    getline(infStream, Symbol, ',');
    getline(infStream, BidPrice, ',');
    getline(infStream, BidSize, ',');
    getline(infStream, AskPrice, ',');
    getline(infStream, AskSize, '\n');


    /*if (PrevTime == Time &&
        PrevSymbol == Symbol)
        continue;*/

    try {
      Feed feed;
      feed.Time = Time;
      feed.Symbol = Symbol;
      feed.BidPrice = atof(BidPrice.c_str());
      feed.BidSize = atoi(BidSize.c_str());
      feed.AskPrice = atof(AskPrice.c_str());
      feed.AskSize = atoi(AskSize.c_str());
      FeedsQueue.push(feed);
    } catch (const exception &e) {
      lock_guard<mutex> out_lck(out_mutex);
      cout << "\nError collecting feed : "
           << e.what();
    }
  }

  Feed feed;
  feed.Symbol = "EndProcess";
  FeedsQueue.push(feed);

}

void dequeue(string Output, ThreadsafeQueue<Feed> &FeedsQueue,
             unordered_map<string, unordered_map<string, shared_ptr<Feed> > > &FeedsMap) {

  while (1) {
    shared_ptr<Feed> feed = FeedsQueue.tryPop();
    if (!feed)
      continue;

    if (feed->Symbol == "EndProcess")
      return;

    // Input the feeds to FeedsMap with symbol as key.
    string Key = feed->Symbol + feed->Time;
    FeedsMap[feed->Symbol][Key] = feed;
  }
}

void readFiles(char *FileName,
               unordered_map<string,
               unordered_map<string, shared_ptr<Feed> > > &FeedsMap) {

  ThreadsafeQueue<Feed> FeedsQueue;
  string Output = string(FileName) + "_Output";

  thread t1(enqueue, FileName, ref(FeedsQueue));
  thread t2(dequeue, Output, ref(FeedsQueue), ref(FeedsMap));
  t1.join();
  t2.join();
}

//=========================================================================
//=========================================================================

//=========================================================================
// Top of the book processing
//========================================================================

void processSymbol(string Symbol, shared_ptr<Feed> &TopOfBook,
                   vector<unordered_map<string,
                   unordered_map<string, shared_ptr<Feed> > > > &FeedsVector) {
  // Sort the feeds for the symbol by time.
  multimap<string, shared_ptr<Feed> > SortedFeeds;
  for (auto &UMap : FeedsVector)
    for (auto &Trade : UMap[Symbol])
      SortedFeeds.insert(make_pair(Trade.first, Trade.second));

  string FileName = "SymbolLog/" + Symbol + ".csv";

  ofstream out;
  try {
    out.open(FileName.c_str());
  } catch (const exception &e) {
    lock_guard<mutex> out_lck(out_mutex);
    cout << "\nError opening output file : "
         << e.what();
  }

  out << "Time" << "," << "Symbol" << ","
      << "BidPrice" << "," << "BidSize"
      << "," << "AskPrice" << "," << "AskSize"
      << "\n";


#ifdef DEBUG
  string debug = FileName + "_debug";
  ofstream dbg(debug.c_str());
#endif

  map<string, shared_ptr<Feed> >::iterator it = SortedFeeds.begin(),
    end = SortedFeeds.end();

  shared_ptr<Feed> BottomOfBook(new Feed);
  unsigned long UpdateCount = 0;

  BottomOfBook->Time = TopOfBook->Time = it->second->Time;
  BottomOfBook->Symbol = TopOfBook->Symbol = it->second->Symbol;
  BottomOfBook->BidPrice = TopOfBook->BidPrice = it->second->BidPrice;
  BottomOfBook->BidSize = TopOfBook->BidSize = it->second->BidSize;
  BottomOfBook->AskPrice = TopOfBook->AskPrice = it->second->AskPrice;
  BottomOfBook->AskSize = TopOfBook->AskSize = it->second->AskSize;
  ++it;

  out << TopOfBook->Time << "," << TopOfBook->Symbol << ","
      << TopOfBook->BidPrice << "," << TopOfBook->BidSize
      << "," << TopOfBook->AskPrice << "," << TopOfBook->AskSize
      << "\n";

#ifdef DEBUG
  dbg << TopOfBook->Time << "," << TopOfBook->Symbol << ","
      << TopOfBook->BidPrice << "," << TopOfBook->BidSize
      << "," << TopOfBook->AskPrice << "," << TopOfBook->AskSize
      << "\n";
#endif

  for (; it != end; ++it) {
#ifdef DEBUG
    pair<string, shared_ptr<Feed> > Trade = *it;
    dbg << Trade.second->Time << "," << Trade.second->Symbol << ","
        << Trade.second->BidPrice << "," << Trade.second->BidSize
        << "," << Trade.second->AskPrice << "," << Trade.second->AskSize
        << "\n";
#endif
    shared_ptr<Feed> CurrentFeed(it->second);
    bool Change = false;
    if (TopOfBook->BidPrice < CurrentFeed->BidPrice) {
      Change = true;
      TopOfBook->BidPrice = CurrentFeed->BidPrice;
      TopOfBook->BidSize = CurrentFeed->BidSize;
      TopOfBook->Time = CurrentFeed->Time;
    } else if (TopOfBook->BidPrice == CurrentFeed->BidPrice) {
      Change = true;
      TopOfBook->BidSize += CurrentFeed->BidSize;
      TopOfBook->Time = CurrentFeed->Time;     
    }

    if (TopOfBook->AskPrice > CurrentFeed->AskPrice) {
      Change = true;
      TopOfBook->AskPrice = CurrentFeed->AskPrice;
      TopOfBook->AskSize = CurrentFeed->AskSize;
      TopOfBook->Time = CurrentFeed->Time;
    } else if (TopOfBook->AskPrice == CurrentFeed->AskPrice) {
      Change = true;
      TopOfBook->AskSize += CurrentFeed->AskSize;
      TopOfBook->Time = CurrentFeed->Time;
    }

    if (BottomOfBook->BidPrice > CurrentFeed->BidPrice) {
      BottomOfBook->BidPrice = CurrentFeed->BidPrice;
      BottomOfBook->BidSize = CurrentFeed->BidSize;
      BottomOfBook->Time = CurrentFeed->Time;
    } else if (BottomOfBook->BidPrice == CurrentFeed->BidPrice) {
      BottomOfBook->BidSize += CurrentFeed->BidSize;
      BottomOfBook->Time = CurrentFeed->Time;      
    }

    if (BottomOfBook->AskPrice < CurrentFeed->AskPrice) {
      BottomOfBook->AskPrice = CurrentFeed->AskPrice;
      BottomOfBook->AskSize = CurrentFeed->AskSize;
      BottomOfBook->Time = CurrentFeed->Time;
    } else if (BottomOfBook->AskPrice == CurrentFeed->AskPrice) {
      BottomOfBook->AskSize += CurrentFeed->AskSize;
      BottomOfBook->Time = CurrentFeed->Time;
    }

    if (Change) {
      ++UpdateCount;
      out << TopOfBook->Time << "," << TopOfBook->Symbol << ","
          << TopOfBook->BidPrice << "," << TopOfBook->BidSize
          << "," << TopOfBook->AskPrice << "," << TopOfBook->AskSize
          << "\n";
    }
  }

  SortedFeeds.clear();
  // Print summary
  out << "\n\nSummary\n\n";
  out << "\nupdate count," << UpdateCount << "\n";
  out << BottomOfBook->Time << "," << BottomOfBook->Symbol << ","
      << BottomOfBook->BidPrice << "," << BottomOfBook->BidSize
      << "," << BottomOfBook->AskPrice << "," << BottomOfBook->AskSize
      << "\n";

  // Clear symbol data.
  for (auto &UMap : FeedsVector)
    UMap[Symbol].clear();

#ifdef DEBUG
  dbg.close();
#endif

  out.close();
}

int main(int argc, char **argv) {

  // Error check for no input files.
  if (argc < 2) {
    cout << "No input files";
    return -1;
  }
  vector<unordered_map<string,
                       unordered_map<string, shared_ptr<Feed> > > > FeedsMap(argc - 1);
  vector<thread> Threads;
  for (int i = 1; i < argc; ++i) {
    thread t(readFiles, argv[i], ref(FeedsMap[i - 1]));
    Threads.push_back(std::move(t));
  }

  for (auto &t : Threads)
    t.join();

  Threads.clear();

  // Make sure to collect all the symbols from all inputs.
  unordered_set<string> SymbolList;
  for (auto &UMap : FeedsMap)
    for (auto &Entry : UMap)
      SymbolList.insert(Entry.first);

  // Build composite top of the book.
  size_t Size = SymbolList.size();
  vector<shared_ptr<Feed> > TopOfBook(Size);
  size_t ThreadCount = 0;
  vector<thread> WorkThreads;

  for (auto &Symbol : SymbolList) {
    shared_ptr<Feed> Sptr(new Feed);
    TopOfBook[ThreadCount] = Sptr;
    //processSymbol(Symbol, TopOfBook[i], FeedsMap);
    thread t(processSymbol, Symbol, ref(TopOfBook[ThreadCount]),
             ref(FeedsMap));
    WorkThreads.push_back(move(t));
    ++ThreadCount;
  }

  // Join work threads.
  for (auto &t : WorkThreads)
    t.join();

  ofstream tob("TopOfBook.csv");
  tob << "Time" << "," << "Symbol" << ","
      << "BidPrice" << "," << "BidSize"
      << "," << "AskPrice" << "," << "AskSize"
      << "\n";

  // Print top of the book for all symbols.
  for (auto &Top : TopOfBook)
    tob << Top->Time << "," << Top->Symbol << ","
        << Top->BidPrice << "," << Top->BidSize
        << "," << Top->AskPrice << "," << Top->AskSize
        << "\n";

  // Clear data structres.
  for (auto &UMap : FeedsMap)
    UMap.clear();

  FeedsMap.clear();
  TopOfBook.clear();
  tob.close();
  return 0;
}
