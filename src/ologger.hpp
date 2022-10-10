#ifndef ologger
#define ologger
#include <string.h>
#include <iomanip>
#include "ostructs.hpp"
#include "oio.hpp"


class logger{
    private:
    public:
    std::vector<std::string> headers;
    std::string header;
    logger(){};
    logger(std::string objectHeader){
        addHeader(objectHeader);
    };
    void addHeader(std::string in){
        headers.push_back(in);
        header += in;
    }
    void removeHeader(std::string garbage){
        headers.pop_back();
        header="";
        for(std::string headerVal:headers) header += headerVal;
    }
    //default logging, with string 
    void logger::log(std::string message){
        #if LOG
        int size;
        MPI_Comm_size(MPI_COMM_WORLD,&size);
        int rank;
        MPI_Comm_rank(MPI_COMM_WORLD,&rank);
        std::string timestamp="";
        #if TIME
        struct timeval tp;
        gettimeofday(&tp,NULL);
        timestamp = " TIME: "+std::to_string(time(0))+"."+std::to_string(tp.tv_usec+1000000).substr(1,6);;
        #endif
        std::string output= "NODE: "+std::to_string(100+rank).substr(1,2)+timestamp+log_Header+message+"\n";
        std::cout<<output;
        #endif
    }
    //with const char* forards to default
    void logger::log(const char* message){
        logger::log((std::string)message);
    }
    //with analysisParameter, parses and forwards to default
    void logger::log(analysisParameter message){
        std::string temp;
        //parseAnalysisParameterToStorageLine(&message,";",&temp);
        logger::log(temp);
    }

    //everything else, converts to string then to default
    template<typename T>
    void logger::log(T message){
        logger::log(std::to_string(message));
        Logger()
    }
};
   
logger defaultLogger;
void initDefaultLogger(){
    defaultLogger = logger();
}
template<typename T>
void Logger(T in){
    defaultLogger.log(in);
}
void AddHeader(){AddHeader("");};
void AddHeader(std::string in){
    defaultLogger.headers.push_back(in);
    defaultLogger.header += in;
}
void RemoveHeader(){RemoveHeader("");};
void RemoveHeader(std::string garbage){
    defaultLogger.headers.pop_back();
    defaultLogger.header="";
    for(std::string headerVal:defaultLogger.headers) defaultLogger.header += headerVal;
}

#endif