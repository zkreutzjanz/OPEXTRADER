
#include <stdio.h>
#include <math.h>  
#include <cmath>
#include <stddef.h>
#include <string.h>
#include <vector>
#include <time.h>
#include <ctime>
#include <iomanip>
#include <fstream>
#include <iostream>
#include <algorithm>
#include <sys/time.h>
#include <mpi.h>

#define LONG_LONG_MIN -__LONG_LONG_MAX__-1
#define LONG_LONG_MAX __LONG_LONG_MAX__


//Constants and Definintions
////////////////////////////////////////////////////////////////////////////////////////////////////
#define LOG 1
#define TIME 1
#define MULTITHREAD_PARENT_ON 1
#define MULTITHREAD_PARENT_OFF 0
const int TICKER_NAME_LENGTH =8;
const int LINE_LOG_LENGTH=100000;
const long long DAY_VALUE =86400;
struct datapoint{
    unsigned long long id=0;
    char name[TICKER_NAME_LENGTH]={' '};
    long long time;
    double open;
    double close;
    double adjClose;
    double change;
};
MPI_Datatype datapointDatatype;
struct ticker{
    unsigned long long id=0;
    int clusterStart;
    int clusterEnd;
    double mean;
    double std;
};
MPI_Datatype tickerDatatype;
struct dayAfterIncStrategyData{
    ticker base;
    ticker target;
    double baseMinBound;
    double baseMaxBound;
    double targetMinBound;
    double targetMaxBound;
    long long timeMinBound;
    long long timeMaxBound;
    int totalTarget;
    int totalTargetInBound;
    int totalBase;
    int totalBaseInBound;
    double likelihood;
    double zScore;
    int dayDifference;
    double responseVal;
};
MPI_Datatype dayAfterIncStrategyDatatype;
struct analysisParameter{
    ticker base;
    ticker target;
    int baseTotal=0;
    int baseInBound=0;
    int targetTotal;
    int targetInBound;
    double baseMin=__DBL_MAX__;
    double baseMax=__DBL_MIN__;
    double baseBoundMargin;
    double targetMin=__DBL_MAX__;
    double targetMax=__DBL_MIN__;
    double targetZScore;
    double targetPercentInc;
    long long baseDayDiff=0;//work on that
    long long targetDayDiff=0;
    
    long long timeMin=LONG_LONG_MIN;
    long long timeMax=LONG_LONG_MAX;
    long long targetDayOffset;
};
MPI_Datatype analysisParameterDatatype;
////////////////////////////////////////////////////////////////////////////////////////////////////

std::vector<ticker> globalTickers;
std::vector<datapoint> globalDatapoints;
int rank;
int size;
std::vector<std::string> log_Header_Vector;
std::string log_Header;
std::string findNameFromID(long long id){
    for(datapoint d: globalDatapoints){
        if(d.id==id){
            return d.name;
        }
    }
    return "";
}

unsigned long long convertNameToID(std::string in){
    int charsToPushFromName = in.size()>TICKER_NAME_LENGTH?TICKER_NAME_LENGTH:in.size();
    char name[TICKER_NAME_LENGTH] = {' '};
    unsigned long long out=0;
    if(charsToPushFromName>0){
		for(int i=0;i<charsToPushFromName;i++){name[i] = in.at(i);}
        for(int i =0;i<TICKER_NAME_LENGTH;i++) {out |= (name[i] << (i*8));}
    }
    return out;
}

//FIX, not efficient
//Globals
////////////////////////////////////////////////////////////////////////////////////////////////////

////////////////////////////////////////////////////////////////////////////////////////////////////
void parseReportToStorageLine(dayAfterIncStrategyData *in,std::string delimiter,std::string *out);
void parseAnalysisParameterToStorageLine(analysisParameter *in,std::string delimiter,std::string *out);
/**
 * @brief For implicit compile time logging
 * @param message std::string message to log
 */

void logger(std::string message){
    #if LOG 
    std::string timestamp="";
    #endif
    #if TIME
    struct timeval tp;
    gettimeofday(&tp,NULL);
    timestamp = " TIME: "+std::to_string(time(0))+"."+std::to_string(tp.tv_usec+1000000).substr(1,6);;
    //timestamp = " TIME: "+std::to_string(time(0));
    #endif
	#if LOG
	int size;
	MPI_Comm_size(MPI_COMM_WORLD,&size);
	int rank;
	MPI_Comm_rank(MPI_COMM_WORLD,&rank);
    std::string output= "NODE: "+std::to_string(100+rank).substr(1,2)+timestamp+log_Header+message+"\n";
    
    
	 std::cout<<output;
	#endif
}
void logger(const char* message){
    logger((std::string)message);
}
void logger(dayAfterIncStrategyData message){
    std::string temp;
    parseReportToStorageLine(&message,";",&temp);
    logger(temp);
}
void logger(analysisParameter message){
    std::string temp;
    parseAnalysisParameterToStorageLine(&message,";",&temp);
    logger(temp);
}

template<typename T>
void logger(T message){
    logger(std::to_string(message));
}


/**
 * @brief adds header to logs
 * @param in std::string header to add
 */
void addHeader(std::string in){
    log_Header_Vector.push_back(in);
    log_Header += in;
   // logger("~~START~~");
}

/**
 * @brief Removes most recent header from logs
 * @param garbage not used
 */

void removeHeader(std::string garbage){
    log_Header_Vector.pop_back();
   // logger("~~END~~");
    log_Header="";
    for(std::string log_Header_Val:log_Header_Vector) log_Header += log_Header_Val;
}
void removeHeader(){ removeHeader("");}

/**
 * @brief Takes a SourceCSV-type string [line] and converts it to a *datapoint[in] based on [delimiter]
 * @param out datapoint out
 * @param line string line in
 * @param delimiter string delimiter
 * @return bool true std::string line successfully parsed to datapoint *out OR
 * @return bool false std::string line could not be parsed to datapoint *out
 */
bool parseSourceCSVLineToDataPoint(datapoint *out,std::string line,std::string delimiter){
	std::string name;
	struct std::tm tm ={0};
    long long time;
	double open=0;
	double close=0;
    double adjClose =0;
	double change=0;
	try{
    //ticker,open,close,aclose,low,high,volume,date
	name = line.substr(0,line.find(delimiter));line.erase(0,line.find(delimiter)+1);//ticker
    open = stod(line.substr(0,line.find(delimiter)));line.erase(0,line.find(delimiter)+1);//open
    close = stod(line.substr(0,line.find(delimiter)));line.erase(0,line.find(delimiter)+1);//close
    adjClose =stod(line.substr(0,line.find(delimiter))); line.erase(0,line.find(delimiter)+1);//adjclose
    line.erase(0,line.find(delimiter)+1);//low
    line.erase(0,line.find(delimiter)+1);//high
    line.erase(0,line.find(delimiter)+1);//volume
        std::istringstream ss(line);
        ss >> std::get_time(&tm, "%Y-%m-%d"); 
    time= mktime(&tm);//time
	}catch(std::exception e){}
	
	int charsToPushFromName = name.size()>TICKER_NAME_LENGTH?TICKER_NAME_LENGTH:name.size();
    if(open&&close&&charsToPushFromName>0){
			for(int i=0;i<charsToPushFromName;i++)
				out->name[i] = name.at(i);
            for(int i =0;i<TICKER_NAME_LENGTH;i++)
                out->id |= (out->name[i] << (i*8));
			out->close = close;
			out->open = open;
            out->adjClose=adjClose;
			out->change = (close/open)-1;
            out->time = time;
            return true;
	}else return false;
}

/**
 * @brief Takes a Storage-type string [line] and converts it to a *datapoint[in] based on [delimiter]
 * @param out datapoint out
 * @param line string line in
 * @param delimiter string delimiter
 * @return bool true std::string line successfully parsed to datapoint *out OR
 * @return bool false std::string line could not be parsed to datapoint *out
 */
bool parseStorageLineToDataPoint(datapoint *out,std::string line,std::string delimiter){
    
    unsigned long long id=0;
    std::string name;
    long long time;
    struct std::tm tm ={0};
    double open=0;
    double close=0;
    double adjClose=0;
    double change=0;

	try{
    //id,name,time,open,close,change
    id = stoll(line.substr(0,line.find(delimiter)));line.erase(0,line.find(delimiter)+1);//id
	name = line.substr(0,line.find(delimiter));line.erase(0,line.find(delimiter)+1);//name
    time = stoll(line.substr(0,line.find(delimiter)));line.erase(0,line.find(delimiter)+1);//time
    open = stod(line.substr(0,line.find(delimiter)));line.erase(0,line.find(delimiter)+1);//open
    close = stod(line.substr(0,line.find(delimiter)));line.erase(0,line.find(delimiter)+1);//close
    adjClose = stod(line.substr(0,line.find(delimiter)));line.erase(0,line.find(delimiter)+1);//adjclose
    change = stod(line);
	}catch(std::exception e){}
	
	int charsToPushFromName = name.size()>TICKER_NAME_LENGTH?TICKER_NAME_LENGTH:name.size();
    if(charsToPushFromName>0){
			for(int i=0;i<charsToPushFromName;i++)
				out->name[i] = name.at(i);
            out->id = id;
			out->close = close;
            out->adjClose=adjClose;
			out->open = open;
			out->change = (close/open)-1;
            out->time = time;
			return true;
	}else return false;

}

/**
 * @brief Takes a Storage-type string [line] and converts it to a *ticker[in] based on [delimiter]
 * @param out ticker out
 * @param line string line in
 * @param delimiter string delimiter
 * @return bool true std::string line successfully parsed to ticker *out OR
 * @return bool false std::string line could not be parsed to ticker *out
 */
bool parseStorageLineToTicker(ticker *out,std::string line,std::string delimiter){
    
    unsigned long long id=0;
    int start;
    int end;
    double mean;
    double std;

	try{
    //id,name,time,open,close,change
    id = stoll(line.substr(0,line.find(delimiter)));line.erase(0,line.find(delimiter)+1);//id
    start = stoi(line.substr(0,line.find(delimiter)));line.erase(0,line.find(delimiter)+1);//start
    end = stoi(line.substr(0,line.find(delimiter)));line.erase(0,line.find(delimiter)+1);//end
    mean = stod(line.substr(0,line.find(delimiter)));line.erase(0,line.find(delimiter)+1);//mean
    std = stod(line); //std
	}catch(std::exception e){}
	
	
    if(id>0){
            out->id = id;
			out->clusterStart = start;
			out->clusterEnd = end;
			out->mean = mean;
            out->std = std;
			return true;
	}else return false;

}

/**
 * @brief Takes a datapoint[in] and converts it to a Storage-type string [line] with [delimiter]
 * @param in datapoint in
 * @param delimiter the string delimiter
 * @param *out std::string output line pointer
 */
void parseDataPointToStorageLine(datapoint *in,std::string delimiter,std::string *out){
    out->append(std::to_string(in->id)+delimiter+(std::string)in->name+delimiter+std::to_string(in->time)+delimiter+std::to_string(in->open)+delimiter+std::to_string(in->close)+delimiter+std::to_string(in->adjClose)+delimiter+std::to_string(in->change)+"\n");
}

/**
 * @brief Takes a string[in] and converts it to a Storage-type string [line] with [delimiter]
 * @param in string in
 * @param delimiter the string delimiter
 * @param *out std::string output line pointer
 */
void parseStringToStorageLine(std::string *in,std::string delimiter,std::string *out){
    out->append(*in+"\n");
}

/**
 * @brief Takes a *ticker[in] and converts it to a Storage-type *string [line] with [delimiter]
 * @param *in ticker in
 * @param delimiter the string delimiter
 * @param *out std::string output line pointer
 */
void parseTickerToStorageLine(ticker *in,std::string delimiter,std::string *out){
    out->append(std::to_string(in->id)+delimiter+std::to_string(in->clusterStart)+delimiter+std::to_string(in->clusterEnd)+delimiter+std::to_string(in->mean)+delimiter+std::to_string(in->std)+"\n");
}

/**
 * @brief Takes a *dayAfterIncStrategyData[in] and converts it to a Storage-type *string [line] with [delimiter]
 * @param *in dayAfterIncStrategyData in
 * @param delimiter the string delimiter
 * @param *out std::string output line pointer
 */
void parseReportToStorageLine(dayAfterIncStrategyData *in,std::string delimiter,std::string *out){
    out->append(std::to_string(in->base.id)+delimiter+std::to_string(in->target.id)+delimiter+std::to_string(in->baseMinBound)+delimiter+std::to_string(in->baseMaxBound)+delimiter+std::to_string(in->targetMinBound)+delimiter+std::to_string(in->targetMaxBound)+delimiter+std::to_string(in->timeMinBound)+delimiter+std::to_string(in->timeMaxBound)+delimiter+std::to_string(in->totalTarget)+delimiter+std::to_string(in->totalTargetInBound)+delimiter+std::to_string(in->totalBase)+delimiter+std::to_string(in->totalBaseInBound)+delimiter+std::to_string(in->dayDifference)+delimiter+std::to_string(in->likelihood)+delimiter+std::to_string(in->zScore)+delimiter+std::to_string(in->responseVal)+"\n");
}

/**
 * @brief Takes a *AnalysisParameter[in] and converts it to a Storage-type *string [line] with [delimiter]
 * @param *in AnalysisParameter in
 * @param delimiter the string delimiter
 * @param *out std::string output line pointer
 */
void parseAnalysisParameterToStorageLine(analysisParameter *in,std::string delimiter,std::string *out){
    out->append(std::to_string(in->base.id)+delimiter+std::to_string(in->target.id)+delimiter+findNameFromID(in->base.id)+delimiter+findNameFromID(in->target.id)+delimiter+std::to_string(in->targetInBound)+delimiter+std::to_string(in->targetTotal)+delimiter+std::to_string(in->targetZScore)+"\n");
}
/**
 * @brief Loads file of name [fileName] to *vector<T> [input] via lineParseFunction
 * @tparam T the type of data being processed to vector
 * @param fileName *std::string filename to load data from
 * @param output *vector<T> to output data to
 * @param lineParseFunction bool function to parse *line to *T with string delimiter
 */
template<typename T>
void loadFileToDataStructure(std::string *fileName,std::vector<T> *output,bool lineParseFunction(T *,std::string,std::string)){
    std::ifstream myFile;
	if(rank==0) logger("opening file <"+*fileName+">");
	myFile.open(*fileName);
	if(rank==0) logger("file <"+*fileName+"> opened");
	std::string line;
	#if LOG
	int linesParsed=0; 
	#endif
	while(getline(myFile, line))
    {
        T temp;
        bool readable = lineParseFunction(&temp,line,",");
        if(readable) output->push_back(temp);
        
		#if LOG
        if(!readable) logger("~~~ERROR~~~ Parse Issue on <"+line+">");
		if(readable) linesParsed++;
		if(linesParsed%LINE_LOG_LENGTH==0) logger("Parsed <"+std::to_string(linesParsed)+"> Lines");
		#endif
    }
    #if LOG
    if(rank==0) logger("<"+std::to_string(linesParsed)+"> Lines Parsed");
    #endif
    myFile.close();
}

/**
 * @brief Loads *vector<T> [input] to file of name [fileName] via datastructParseFunction
 * @tparam T the type of data being processed from vector
 * @param fileName *std::string filename to load data into
 * @param input *vector<T> of input data
 * @param datastructParseFunction bool function to parse *T to *line with string delimiter
 */
template<typename T>
void loadDatastructsToFile(std::string *fileName,std::vector<T> *input,void datastructParseFunction(T *,std::string,std::string *)){
	if(rank==0) logger("opening file <"+*fileName+">");
    MPI_File myFile;
    char fntmp[fileName->length()];
    strcpy(fntmp,fileName->c_str());
    MPI_Barrier(MPI_COMM_WORLD);
    MPI_File_delete(fntmp,MPI_INFO_NULL);
    MPI_File_open(MPI_COMM_WORLD,fntmp,MPI_MODE_RDWR|MPI_MODE_CREATE,MPI_INFO_NULL,&myFile);
    
	if(rank==0) logger("file <"+*fileName+"> opened");
    #if LOG
	int linesWritten=0; 
	#endif
    if(rank==0){
    for(int i = 0;i<input->size();i++){
        std::string temp="";
        datastructParseFunction(&input->at(i),",",&temp);
        
        char tempChar[temp.length()];
        strcpy(tempChar, temp.c_str());
        MPI_Status testStatus;
        MPI_File_write(myFile,tempChar,temp.length(),MPI_CHAR,&testStatus);
        #if LOG
		linesWritten++;
		if(linesWritten%LINE_LOG_LENGTH==0) logger("wrote <"+std::to_string(linesWritten)+"> Lines");
		#endif
    }}
    #if LOG
   if(rank==0) logger("<"+std::to_string(linesWritten)+"> Lines Written");
    #endif
    
    MPI_File_close(&myFile);
}
////////////////////////////////////////////////////////////////////////////////////////////////////


void declareDatatypes(){

	int lengths[] = {1,TICKER_NAME_LENGTH,1,1,1,1,1};
	MPI_Aint disps[] = {offsetof(datapoint,id),offsetof(datapoint,name),offsetof(datapoint,time),offsetof(datapoint,open),offsetof(datapoint,close),offsetof(datapoint,adjClose),offsetof(datapoint,change)};
	MPI_Datatype types[] = {MPI_LONG_LONG,MPI_CHAR,MPI_LONG_LONG,MPI_DOUBLE,MPI_DOUBLE,MPI_DOUBLE,MPI_DOUBLE};
	MPI_Type_create_struct(7,lengths,disps,types,&datapointDatatype);
	MPI_Type_commit(&datapointDatatype);

    int tlengths[] = {1,1,1,1,1};
	MPI_Aint tdisps[] = {offsetof(ticker,id),offsetof(ticker,clusterStart),offsetof(ticker,clusterEnd),offsetof(ticker,mean),offsetof(ticker,std)};
	MPI_Datatype ttypes[] = {MPI_LONG_LONG,MPI_INT,MPI_INT,MPI_DOUBLE,MPI_DOUBLE};
	MPI_Type_create_struct(5,tlengths,tdisps,ttypes,&tickerDatatype);
	MPI_Type_commit(&tickerDatatype);

    int rlengths[] = {1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1};
	MPI_Aint rdisps[] = {offsetof(dayAfterIncStrategyData,base),offsetof(dayAfterIncStrategyData,target),offsetof(dayAfterIncStrategyData,baseMinBound),offsetof(dayAfterIncStrategyData,baseMaxBound),offsetof(dayAfterIncStrategyData,targetMinBound),offsetof(dayAfterIncStrategyData,targetMaxBound),offsetof(dayAfterIncStrategyData,timeMinBound),offsetof(dayAfterIncStrategyData,timeMaxBound),offsetof(dayAfterIncStrategyData,totalTarget),offsetof(dayAfterIncStrategyData,totalTargetInBound),offsetof(dayAfterIncStrategyData,totalBase),offsetof(dayAfterIncStrategyData,totalBaseInBound),offsetof(dayAfterIncStrategyData,likelihood),offsetof(dayAfterIncStrategyData,zScore),offsetof(dayAfterIncStrategyData,dayDifference),offsetof(dayAfterIncStrategyData,responseVal)};
	MPI_Datatype rtypes[] = {tickerDatatype,tickerDatatype,MPI_DOUBLE,MPI_DOUBLE,MPI_DOUBLE,MPI_DOUBLE,MPI_LONG_LONG,MPI_LONG_LONG,MPI_INT,MPI_INT,MPI_INT,MPI_INT,MPI_DOUBLE,MPI_DOUBLE,MPI_INT,MPI_DOUBLE};
	MPI_Type_create_struct(16,rlengths,rdisps,rtypes,&dayAfterIncStrategyDatatype);
	MPI_Type_commit(&dayAfterIncStrategyDatatype);

    int aplengths[] = {1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1};
	MPI_Aint apdisps[] = {offsetof(analysisParameter,base),offsetof(analysisParameter,target),offsetof(analysisParameter,baseTotal),offsetof(analysisParameter,baseInBound),offsetof(analysisParameter,targetTotal),offsetof(analysisParameter,targetInBound),offsetof(analysisParameter,baseMin),offsetof(analysisParameter,baseMax),offsetof(analysisParameter,baseBoundMargin),offsetof(analysisParameter,targetMin),offsetof(analysisParameter,targetMax),offsetof(analysisParameter,targetZScore),offsetof(analysisParameter,targetPercentInc),offsetof(analysisParameter,baseDayDiff),offsetof(analysisParameter,targetDayDiff),offsetof(analysisParameter,timeMin),offsetof(analysisParameter,timeMax),offsetof(analysisParameter,targetDayOffset)};
	MPI_Datatype aptypes[] = {tickerDatatype,tickerDatatype,MPI_INT,MPI_INT,MPI_INT,MPI_INT,MPI_DOUBLE,MPI_DOUBLE,MPI_DOUBLE,MPI_DOUBLE,MPI_DOUBLE,MPI_DOUBLE,MPI_DOUBLE,MPI_DOUBLE,MPI_LONG_LONG,MPI_LONG_LONG,MPI_LONG_LONG,MPI_LONG_LONG,MPI_LONG_LONG};
	MPI_Type_create_struct(18,aplengths,apdisps,aptypes,&analysisParameterDatatype);
	MPI_Type_commit(&analysisParameterDatatype);

}

void shareDatapoints(){
    int finalTickerCount = globalTickers.size();
    MPI_Bcast(&finalTickerCount,1,MPI_INT,0,MPI_COMM_WORLD);
    int finalDatapointCount = globalDatapoints.size();
    MPI_Bcast(&finalDatapointCount,1,MPI_INT,0,MPI_COMM_WORLD);
    if(rank>0) globalTickers.resize(finalTickerCount);
    MPI_Bcast(globalTickers.data(),finalTickerCount,tickerDatatype,0,MPI_COMM_WORLD);
    if(rank>0) globalDatapoints.resize(finalDatapointCount);
    MPI_Bcast(&(globalDatapoints[0]),finalDatapointCount,datapointDatatype,0,MPI_COMM_WORLD);
}

void identifyTickersFromDatapoints(std::vector<datapoint> *in, std::vector<ticker> *out){
     #if LOG
	int tickersIdentified=0; 
	#endif

    for(int i =0;i<in->size();i++){
        bool found = false;
        int j=0;
        while(!found&&j<out->size()){
            if(in->at(i).id==out->at(j).id) found =true;
            j++;
        }
        if(!found){
            ticker newticker;
            newticker.id=in->at(i).id;
            out->push_back(newticker);
             #if LOG
		    tickersIdentified++;
		    if(tickersIdentified%100==0) logger("Identified <"+std::to_string(out->size())+"> Tickers");
		    #endif
        }
       
    }
}

void generateTickerSchema(std::vector<datapoint> *in,std::vector<ticker> *out){
    ticker currentTicker;
    currentTicker.clusterStart=0;
    currentTicker.id=in->at(0).id;
    double sum=in->at(0).change;
    for(int i=1;i<in->size();i++){
        if(in->at(i).id!=currentTicker.id){
            currentTicker.clusterEnd=i-1;
            currentTicker.mean=(double)sum/(double)(currentTicker.clusterEnd-currentTicker.clusterStart+1);
            
            double stdSum=0;
            for(int j = currentTicker.clusterStart;j<currentTicker.clusterEnd+1;j++){
                stdSum += (in->at(j).change-currentTicker.mean)*(in->at(j).change-currentTicker.mean);
               // log("INNER LOOP:::"+std::to_string((in->at(j).change-currentTicker.mean)*(in->at(j).change-currentTicker.mean))+":::"+std::to_string(in->at(j).change)+"::"+std::to_string(currentTicker.mean));
            
            }
            currentTicker.std= sqrt(stdSum/(double)(currentTicker.clusterEnd-currentTicker.clusterStart+1));
            logger(std::to_string(i)+":::"+std::to_string(sum)+"::"+std::to_string(stdSum)+"::"+std::to_string(currentTicker.clusterEnd-currentTicker.clusterStart+1)+"::"+std::to_string(currentTicker.mean)+"::"+std::to_string(currentTicker.std));
            out->push_back(currentTicker);
            currentTicker.id=in->at(i).id;
            currentTicker.clusterStart=i;
            currentTicker.clusterEnd=0;
            currentTicker.mean=0;
            currentTicker.std=0;
            sum=in->at(i).change;
        }
        sum += in->at(i).change;
    }
}

double getZScore (double p)
{
    // Lower tail quantile for standard normal distribution function.
    //
    // This function returns an approximation of the inverse cumulative
    // standard normal distribution function.  I.e., given P, it returns
    // an approximation to the X satisfying P = Pr{Z <= X} where Z is a
    // random variable from the standard normal distribution.
    //
    // The algorithm uses a minimax approximation by rational functions
    // and the result has a relative error whose absolute value is less
    // than 1.15e-9.
    //
    // Author:      Peter J. Acklam
    // Time-stamp:  2003-05-05 05:15:14
    // E-mail:      pjacklam@online.no
    // WWW URL:     http://home.online.no/~pjacklam

    // An algorithm with a relative error less than 1.15*10-9 in the entire region.
    
    // Coefficients in rational approximations
    float a[] = { -39.696830f, 220.946098f, -275.928510f, 138.357751f, -30.664798f, 2.506628f };
    
    float b[] = { -54.476098f, 161.585836f, -155.698979f, 66.801311f, -13.280681f };
    
    float c[] = { -0.007784894002f, -0.32239645f, -2.400758f, -2.549732f, 4.374664f, 2.938163f };
    
    float d[] = { 0.007784695709f, 0.32246712f, 2.445134f, 3.754408f };
    
    // Define break-points.
    float plow = 0.02425f;
    float phigh = 1 - plow;
    
    // Rational approximation for lower region:
    if ( p < plow ) {
        float q = sqrt( -2 * log( p ) );
        return ( ( ( ( ( c[ 0 ] * q + c[ 1 ] ) * q + c[ 2 ] ) * q + c[ 3 ] ) * q + c[ 4 ] ) * q + c[ 5 ] ) /
        ( ( ( ( d[ 0 ] * q + d[ 1 ] ) * q + d[ 2 ] ) * q + d[ 3 ] ) * q + 1 );
    }
    
    // Rational approximation for upper region:
    if ( phigh < p ) {
        float q = sqrt( -2 * log( 1 - p ) );
        return -( ( ( ( ( c[ 0 ] * q + c[ 1 ] ) * q + c[ 2 ] ) * q + c[ 3 ] ) * q + c[ 4 ] ) * q + c[ 5 ] ) /
        ( ( ( ( d[ 0 ] * q + d[ 1 ] ) * q + d[ 2 ] ) * q + d[ 3 ] ) * q + 1 );
    }
    
    // Rational approximation for central region:
    {
        float q = p - 0.5f;
        float r = q * q;
        return ( ( ( ( ( a[ 0 ] * r + a[ 1 ] ) * r + a[ 2 ] ) * r + a[ 3 ] ) * r + a[ 4 ] ) * r + a[ 5 ] ) * q /
        ( ( ( ( ( b[ 0 ] * r + b[ 1 ] ) * r + b[ 2 ] ) * r + b[ 3 ] ) * r + b[ 4 ] ) * r + 1 );
    }
}

float getPercentile(double z){
    return (1.0-erf(-(z)/ sqrt(2.0)))/2.0;
}

double dayAfterAmIRightStrategy(int MULTITHREAD_MODE,dayAfterIncStrategyData dataTemplate,std::vector<dayAfterIncStrategyData> *output,long long testDate,double zCutoff){
    
    
    /**
     * @brief The generatingBaseTickerData Section (O(globalDatapoints.size()))
     * Gets all tickers from the given testdate and puts them into an array of dataTemplates.
     * Added to the datatemplate:
     *  A copy of the ticker
     *  The Base bounds computed via the testdates inc for the ticker, the input datatemplates percentile, and the requestmargin
     */
    addHeader("generatingBaseTickerData::");
    double requestMargin = .025;
    dataTemplate.timeMaxBound=testDate;
    std::vector<dayAfterIncStrategyData> baseTickerData;
    for(ticker t:globalTickers){
        for(int i=t.clusterStart;i<t.clusterEnd+1;i++){
            if(globalDatapoints.at(i).time==testDate-DAY_VALUE*dataTemplate.dayDifference){
                dayAfterIncStrategyData newBaseTicker = dataTemplate;
                double percentile = getPercentile((globalDatapoints.at(i).change-t.mean)/t.std);
                newBaseTicker.baseMinBound= getZScore((percentile - requestMargin)<0?.01:(percentile - requestMargin))*t.std+t.mean;
                newBaseTicker.baseMaxBound = getZScore((percentile + requestMargin)>1?.99:(percentile + requestMargin))*t.std+t.mean;
                newBaseTicker.base=t;
                baseTickerData.push_back(newBaseTicker);
                break;
            }
        }
    }
    removeHeader("generatingBaseTickerData::");
    
    /**
     * @brief The identifyBaseTickerResponsibiliy
     * Decides which baseTicker datatemplates the current node needs to worry about.
     */
    addHeader("identifyBaseTickerResponsibiliy::");
    int baseTickerStart=0;
    int baseTickerEnd=0;
    if(MULTITHREAD_MODE==MULTITHREAD_PARENT_OFF){
        if(baseTickerData.size()<size){
            baseTickerStart=rank;
            baseTickerEnd= baseTickerData.size()>rank?rank+1:rank;
        }else{
            int partition = baseTickerData.size()/size;
            baseTickerStart=partition*rank;
            baseTickerEnd= (rank+1==size)?baseTickerData.size():partition*(rank+1);
        }
    }else{
        baseTickerStart=0;
        baseTickerEnd=baseTickerData.size();
    }
    removeHeader();


    /**
     * @brief The generatingTargetTickerData Section
     * For every Base ticker datatemplate a datatemplate is made for every target ticker, 
     *  filtered by if they are seen as profittable based on preset target bounds, z score, and likelihood from template
     */
    addHeader("generatingTargetTickerData::");
    std::vector<dayAfterIncStrategyData> localTargetTickerData;
    for(int i = baseTickerStart;i<baseTickerEnd;i++){

        //finds all datapoints from base ticker in base bound
        std::vector<datapoint> basesInBound;
        baseTickerData.at(i).totalBase=0;
        for(int baseID = baseTickerData.at(i).base.clusterStart; baseID<baseTickerData.at(i).base.clusterEnd+1;baseID++){
            if(globalDatapoints.at(baseID).time>=baseTickerData.at(i).timeMinBound
             &&globalDatapoints.at(baseID).time<baseTickerData.at(i).timeMaxBound-DAY_VALUE*dataTemplate.dayDifference){
                baseTickerData.at(i).totalBase++;
                if(globalDatapoints.at(baseID).change>=baseTickerData.at(i).baseMinBound
                    &&globalDatapoints.at(baseID).change<=baseTickerData.at(i).baseMaxBound){
                    basesInBound.push_back(globalDatapoints.at(baseID));
                }
            }
        }
        baseTickerData.at(i).totalBaseInBound=basesInBound.size();
     



        //goes through all possible target tickers
        for(ticker tt:globalTickers){
            if(tt.id==baseTickerData.at(i).base.id) continue;
            int ttStart = tt.clusterStart;
            dayAfterIncStrategyData targetTickerData = baseTickerData.at(i);
            targetTickerData.totalTarget=0;
            targetTickerData.totalTargetInBound=0;
            for(datapoint bd:basesInBound){
                for(int td=ttStart;td<tt.clusterEnd+1-dataTemplate.dayDifference;td++){
                    if(globalDatapoints.at(td).time==bd.time){
                        targetTickerData.totalTarget++;
                        if(globalDatapoints.at(td+dataTemplate.dayDifference).change>=baseTickerData.at(i).targetMinBound
                         &&globalDatapoints.at(td+dataTemplate.dayDifference).change<=baseTickerData.at(i).targetMaxBound
                        ){
                            targetTickerData.totalTargetInBound++;
                        }
                        ttStart=td+1;
                        break;
                    }
                }
            }

            //generate stats for the given targettickerdata
            if(!targetTickerData.totalTarget) continue;
            double zout = ((double)((double)targetTickerData.totalTargetInBound/(double)targetTickerData.totalTarget)-targetTickerData.likelihood)/sqrt((1-targetTickerData.likelihood)*targetTickerData.likelihood/targetTickerData.totalTarget);
            if(zout<zCutoff) continue;
            targetTickerData.target = tt;
            targetTickerData.zScore=zout;
            targetTickerData.responseVal=0;
            //get the profit, note:this is sometimes 0
            for(int td=tt.clusterStart;td<tt.clusterEnd+1;td++){
                if(globalDatapoints.at(td).time==testDate) targetTickerData.responseVal=globalDatapoints.at(td).change;
            }
            
            localTargetTickerData.push_back(targetTickerData);
        }
    }
    removeHeader("generatingTargetTickerData::");
   

    /**
     * @brief generatingFinalTargetDataVector
     * If it is multithreaded, combines all local vectors
     * otherwise just copies local vector over
     */
    addHeader("generatingFinalTargetDataVector::");
    std::vector<dayAfterIncStrategyData> finalTargetTickerData;
    if(MULTITHREAD_MODE==MULTITHREAD_PARENT_OFF){
        int localTargetCount = localTargetTickerData.size();
        std::vector<int> localTargetCounts;
        localTargetCounts.resize(size);
        MPI_Gather(&localTargetCount,1,MPI_INT,&localTargetCounts[0],1,MPI_INT,0,MPI_COMM_WORLD);
        std::vector<int> localTargetDispls;
        localTargetDispls.push_back(0);
        int totalTargets= rank==0?localTargetCounts.at(0):0;
        for(int i=1;i<size;i++){
            if(rank==0) totalTargets += localTargetCounts.at(i);
            localTargetDispls.push_back(localTargetDispls.at(i-1)+localTargetCounts.at(i-1));
        }
        finalTargetTickerData.resize(totalTargets);
        MPI_Gatherv(&localTargetTickerData[0],localTargetCount,dayAfterIncStrategyDatatype,(&finalTargetTickerData[0]),&localTargetCounts[0],&localTargetDispls[0],dayAfterIncStrategyDatatype,0,MPI_COMM_WORLD);
    }else{
        finalTargetTickerData= localTargetTickerData;
    }
    removeHeader("generatingFinalTargetDataVector::");

    addHeader("combiningReports::");
    
    int right=0;
    int wrong=0;
    for(dayAfterIncStrategyData r:finalTargetTickerData){
        if(r.responseVal>=r.targetMinBound&&r.responseVal<=r.targetMaxBound){
            right++;
        }else{
            wrong++;
        }
        output->push_back(r);
       
    }
    
    removeHeader("combiningreports::");
    if(right+wrong==0){return 0;}
    if(right/(right+wrong)>=dataTemplate.likelihood){
        return .05;
    }else{
        return -.05;
    }
}

double dayAfterIncStrategy(int MULTITHREAD_MODE,dayAfterIncStrategyData dataTemplate,std::vector<dayAfterIncStrategyData> *output,long long testDate,double zCutoff){
    
    
    /**
     * @brief The generatingBaseTickerData Section (O(globalDatapoints.size()))
     * Gets all tickers from the given testdate and puts them into an array of dataTemplates.
     * Added to the datatemplate:
     *  A copy of the ticker
     *  The Base bounds computed via the testdates inc for the ticker, the input datatemplates percentile, and the requestmargin
     */
    addHeader("generatingBaseTickerData::");
    double requestMargin = .025;
    dataTemplate.timeMaxBound=testDate;
    std::vector<dayAfterIncStrategyData> baseTickerData;
    for(ticker t:globalTickers){
        for(int i=t.clusterStart;i<t.clusterEnd+1;i++){
            if(globalDatapoints.at(i).time==testDate-DAY_VALUE*dataTemplate.dayDifference){
                dayAfterIncStrategyData newBaseTicker = dataTemplate;
                double percentile = getPercentile((globalDatapoints.at(i).change-t.mean)/t.std);
                newBaseTicker.baseMinBound= getZScore((percentile - requestMargin)<0?.01:(percentile - requestMargin))*t.std+t.mean;
                newBaseTicker.baseMaxBound = getZScore((percentile + requestMargin)>1?.99:(percentile + requestMargin))*t.std+t.mean;
                newBaseTicker.base=t;
                baseTickerData.push_back(newBaseTicker);
                break;
            }
        }
    }
    removeHeader("generatingBaseTickerData::");
    
    /**
     * @brief The identifyBaseTickerResponsibiliy
     * Decides which baseTicker datatemplates the current node needs to worry about.
     */
    addHeader("identifyBaseTickerResponsibiliy::");
    int baseTickerStart=0;
    int baseTickerEnd=0;
    if(MULTITHREAD_MODE==MULTITHREAD_PARENT_OFF){
        if(baseTickerData.size()<size){
            baseTickerStart=rank;
            baseTickerEnd= baseTickerData.size()>rank?rank+1:rank;
        }else{
            int partition = baseTickerData.size()/size;
            baseTickerStart=partition*rank;
            baseTickerEnd= (rank+1==size)?baseTickerData.size():partition*(rank+1);
        }
    }else{
        baseTickerStart=0;
        baseTickerEnd=baseTickerData.size();
    }
    removeHeader();


    /**
     * @brief The generatingTargetTickerData Section
     * For every Base ticker datatemplate a datatemplate is made for every target ticker, 
     *  filtered by if they are seen as profittable based on preset target bounds, z score, and likelihood from template
     */
    addHeader("generatingTargetTickerData::");
    std::vector<dayAfterIncStrategyData> localTargetTickerData;
    for(int i = baseTickerStart;i<baseTickerEnd;i++){

        //finds all datapoints from base ticker in base bound
        std::vector<datapoint> basesInBound;
        baseTickerData.at(i).totalBase=0;
        for(int baseID = baseTickerData.at(i).base.clusterStart; baseID<baseTickerData.at(i).base.clusterEnd+1;baseID++){
            if(globalDatapoints.at(baseID).time>=baseTickerData.at(i).timeMinBound
             &&globalDatapoints.at(baseID).time<baseTickerData.at(i).timeMaxBound-DAY_VALUE*dataTemplate.dayDifference){
                baseTickerData.at(i).totalBase++;
                if(globalDatapoints.at(baseID).change>=baseTickerData.at(i).baseMinBound
                    &&globalDatapoints.at(baseID).change<=baseTickerData.at(i).baseMaxBound){
                    basesInBound.push_back(globalDatapoints.at(baseID));
                }
            }
        }
        baseTickerData.at(i).totalBaseInBound=basesInBound.size();
     



        //goes through all possible target tickers
        for(ticker tt:globalTickers){
            if(tt.id==baseTickerData.at(i).base.id) continue;
            int ttStart = tt.clusterStart;
            dayAfterIncStrategyData targetTickerData = baseTickerData.at(i);
            targetTickerData.totalTarget=0;
            targetTickerData.totalTargetInBound=0;
            for(datapoint bd:basesInBound){
                for(int td=ttStart;td<tt.clusterEnd+1-dataTemplate.dayDifference;td++){
                    if(globalDatapoints.at(td).time==bd.time){
                        targetTickerData.totalTarget++;
                        if(globalDatapoints.at(td+dataTemplate.dayDifference).change>=baseTickerData.at(i).targetMinBound
                         &&globalDatapoints.at(td+dataTemplate.dayDifference).change<=baseTickerData.at(i).targetMaxBound
                        ){
                            targetTickerData.totalTargetInBound++;
                        }
                        ttStart=td+1;
                        break;
                    }
                }
            }

            //generate stats for the given targettickerdata
            if(!targetTickerData.totalTarget) continue;
            double zout = ((double)((double)targetTickerData.totalTargetInBound/(double)targetTickerData.totalTarget)-targetTickerData.likelihood)/sqrt((1-targetTickerData.likelihood)*targetTickerData.likelihood/targetTickerData.totalTarget);
            if(zout<zCutoff) continue;
            targetTickerData.target = tt;
            targetTickerData.zScore=zout;
            targetTickerData.responseVal=0;
            //get the profit, note:this is sometimes 0
            for(int td=tt.clusterStart;td<tt.clusterEnd+1;td++){
                if(globalDatapoints.at(td).time==testDate) targetTickerData.responseVal=globalDatapoints.at(td).change;
            }
            
            localTargetTickerData.push_back(targetTickerData);
        }
    }
    removeHeader("generatingTargetTickerData::");
   

    /**
     * @brief generatingFinalTargetDataVector
     * If it is multithreaded, combines all local vectors
     * otherwise just copies local vector over
     */
    addHeader("generatingFinalTargetDataVector::");
    std::vector<dayAfterIncStrategyData> finalTargetTickerData;
    if(MULTITHREAD_MODE==MULTITHREAD_PARENT_OFF){
        int localTargetCount = localTargetTickerData.size();
        std::vector<int> localTargetCounts;
        localTargetCounts.resize(size);
        MPI_Gather(&localTargetCount,1,MPI_INT,&localTargetCounts[0],1,MPI_INT,0,MPI_COMM_WORLD);
        std::vector<int> localTargetDispls;
        localTargetDispls.push_back(0);
        int totalTargets= rank==0?localTargetCounts.at(0):0;
        for(int i=1;i<size;i++){
            if(rank==0) totalTargets += localTargetCounts.at(i);
            localTargetDispls.push_back(localTargetDispls.at(i-1)+localTargetCounts.at(i-1));
        }
        finalTargetTickerData.resize(totalTargets);
        MPI_Gatherv(&localTargetTickerData[0],localTargetCount,dayAfterIncStrategyDatatype,(&finalTargetTickerData[0]),&localTargetCounts[0],&localTargetDispls[0],dayAfterIncStrategyDatatype,0,MPI_COMM_WORLD);
    }else{
        finalTargetTickerData= localTargetTickerData;
    }
    removeHeader("generatingFinalTargetDataVector::");

    addHeader("combiningReports::");
    double sum=0;
    for(dayAfterIncStrategyData r:finalTargetTickerData){
         sum+=r.responseVal;
        output->push_back(r);
       
    }
    removeHeader("combiningreports::");
    
    return finalTargetTickerData.size()?sum/(double)finalTargetTickerData.size():0;
}

double shortTermInvestmentStrategy(int MULTITHREAD_MODE,dayAfterIncStrategyData requestTemplate,std::vector<dayAfterIncStrategyData> *responseData,long long testDate,double zCutoff){
    double changeSum=0;
    double changeCount=0;
    for(ticker t:globalTickers){
        for(int i = t.clusterStart;i<t.clusterEnd+1;i++){
            if(globalDatapoints.at(i).time==testDate){
                changeSum += globalDatapoints.at(i).change;
                changeCount++;
                break;
            }
        }
    }
    if(!changeCount) return 0;
    return changeSum/(double)changeCount;
}

double longTermInvestmentStrategy(int MULTITHREAD_MODE,dayAfterIncStrategyData requestTemplate,std::vector<dayAfterIncStrategyData> *responseData,long long testDate,double zCutoff){
    double changeSum=0;
    double changeCount=0;
    for(ticker t:globalTickers){
        for(int i = t.clusterStart+1;i<t.clusterEnd+1;i++){
            if(globalDatapoints.at(i).time==testDate){
                changeSum += globalDatapoints.at(i).adjClose/globalDatapoints.at(i-1).adjClose-1;
                changeCount++;
                break;
            }
        }
    }
    if(!changeCount) return 0;
    return changeSum/(double)changeCount;
}




//To DO support setting of variable target range



void createBaseBounds(analysisParameter *in,datapoint *testDatapoint){
    //if no bounds are already set, set them based on the test datapoints change value
    if(in->baseMax<in->baseMin){
        if(in->baseBoundMargin==NULL) throw "createBaseBounds::No in->baseBoundMargin set when needed. (baseBounds not set)";
        //now we have to see what type of change it is
        //if it is a daily change do below, we can easily find range
        //  we do have to recompute distr val bc of time limits
        if(!in->baseDayDiff){
            //create the probablity distr for getting z score
            double sumOfChange=0;
            int inTimeRange=0;
            for(int dp=in->base.clusterStart+in->baseDayDiff;dp<in->base.clusterEnd+1;dp++){
                if(globalDatapoints.at(dp).time<=in->timeMax&&globalDatapoints.at(dp).time>=in->timeMin){
                    sumOfChange += globalDatapoints.at(dp).adjClose/globalDatapoints.at(dp-in->baseDayDiff).adjClose-1;
                    inTimeRange++;
                }
            }
            double mean = sumOfChange/(double)(inTimeRange);
            double stdSum =0;
            for(int dp=in->base.clusterStart+in->baseDayDiff;dp<in->base.clusterEnd+1;dp++){
                if(globalDatapoints.at(dp).time<=in->timeMax&&globalDatapoints.at(dp).time>=in->timeMin){
                    double change = globalDatapoints.at(dp).adjClose/globalDatapoints.at(dp-in->baseDayDiff).adjClose-1;
                    stdSum += (change-mean)*(change-mean);
                }
            }
            double std = sqrt(stdSum/(double)(inTimeRange));
            double percentile = getPercentile((testDatapoint->change-in->base.mean)/in->base.std);
            in->baseMin= getZScore((percentile - in->baseBoundMargin)<0?.01:(percentile - in->baseBoundMargin))*in->base.std+in->base.mean;
            in->baseMax = getZScore((percentile + in->baseBoundMargin)>1?.99:(percentile + in->baseBoundMargin))*in->base.std+in->base.mean;
              
        }else{//but in all other cases we must compute a new prob distr

            //now for the hard part, we have to find the z score of the change value for our input 
            //  dp but we do not know if it lies in the tickers dp vector, IE there may be no according multiday 
            //  change if it is a standalone dp
            //SO: we throw an exception if it doesnt lie in the vector, this is bc the program is not supr smart
            // TODO: implement time based consecutivity checking for external data multiday base inc strategies
            //THE common practice for multiday analysis it to then only use in vector data, and no external data
            double testChange=0;
            bool found=false;
            for(int dp=in->base.clusterStart+in->baseDayDiff;dp<in->base.clusterEnd+1;dp++){
                if(globalDatapoints.at(dp).time==testDatapoint->time){ 
                    testChange = globalDatapoints.at(dp-in->baseDayDiff).adjClose/testDatapoint->adjClose-1;
                    found==true;
                }
            }
            if(!found) throw "createBaseBounds:: testdatapoint has no ~ child time value in base dps time values. Why are you inputing an invalid test datapoint for a multi day inc??";
            
            //create the probablity distr for getting z score
            double sumOfChange=0;
            int inTimeRange=0;
            for(int dp=in->base.clusterStart+in->baseDayDiff;dp<in->base.clusterEnd+1;dp++){
                if(globalDatapoints.at(dp).time<=in->timeMax&&globalDatapoints.at(dp).time>=in->timeMin){
                    sumOfChange += globalDatapoints.at(dp).adjClose/globalDatapoints.at(dp-in->baseDayDiff).adjClose-1;
                    inTimeRange++;
                }
            }
            double mean = sumOfChange/(double)(inTimeRange);
            double stdSum =0;
            for(int dp=in->base.clusterStart+in->baseDayDiff;dp<in->base.clusterEnd+1;dp++){
                if(globalDatapoints.at(dp).time<=in->timeMax&&globalDatapoints.at(dp).time>=in->timeMin){
                    double change = globalDatapoints.at(dp).adjClose/globalDatapoints.at(dp-in->baseDayDiff).adjClose-1;
                    stdSum += (change-mean)*(change-mean);
                }
            }
            double std = sqrt(stdSum/(double)(inTimeRange));
            
            //get zscore/generate bounds
            double percentile = getPercentile((testChange-mean)/std);
            in->baseMin= getZScore((percentile - in->baseBoundMargin)<0?.01:(percentile - in->baseBoundMargin))*std+mean;
            in->baseMax = getZScore((percentile + in->baseBoundMargin)>1?.99:(percentile + in->baseBoundMargin))*std+mean;
        }
    }
    //else do nothing, bounds should be set correctly!
}

void findValidBases(analysisParameter *in,std::vector<datapoint> *out){
    for(int dp = in->base.clusterStart-in->baseDayDiff;dp<in->base.clusterEnd+1-in->targetDayOffset;dp++){
        if(globalDatapoints.at(dp-in->baseDayDiff).time>=in->timeMin&&globalDatapoints.at(dp).time<=in->timeMax){
            in->baseTotal++;
            double change = (in->baseDayDiff==0)?globalDatapoints.at(dp).change:globalDatapoints.at(dp-in->baseDayDiff).adjClose/globalDatapoints.at(dp).adjClose-1;
            if(change>=in->baseMin&&change<=in->baseMax){
                in->baseInBound++;
                out->push_back(globalDatapoints.at(dp));
            }
            
        }
    }
}

void backtestAnalysis(analysisParameter in,long long startDate, long long endDate,void analysis(analysisParameter,std::vector<analysisParameter> *,datapoint)){
    for(long long day=startDate;day<endDate+DAY_VALUE;day+DAY_VALUE){
        //first identify which datapoint bases to test on
        for()


    }
}

void runAnalysisOnDay(int MULTITHREAD_MODE,analysisParameter analysisTemplate, double *rightnessRatio,double *estProfit,long long testDay,void analysis(analysisParameter,std::vector<analysisParameter> *)){
    std::vector<analysisParameter> aps;
    
    for(ticker t:globalTickers){
        for(int i =t.clusterStart;i<t.clusterEnd+1;i++){
            if(globalDatapoints.at(i).time==testDay){
                analysisParameter temp = analysisTemplate;
                temp.base=t;
                createBaseBounds(&temp,&globalDatapoints.at(i));
            }
        }
    }

    int firstAP=0;
    int lastAP=aps.size();
    if(MULTITHREAD_MODE==MULTITHREAD_PARENT_OFF){
        int partition = lastAP/size;
        firstAP = rank*partition;
        lastAP = partition*(rank+1)+((rank==size-1)?lastAP-partition*(rank+1):0);
    }

    std::vector<analysisParameter> localResults;
    for(int apID = firstAP;apID<lastAP;apID++){
        safeAnalysis(aps.at(apID),&localResults);
    }

    std::vector<analysisParameter> results;
    if(MULTITHREAD_MODE==MULTITHREAD_PARENT_OFF){
        int localResultsCount = localResults.size();
        std::vector<int> localResultsCounts;
        localResultsCounts.resize(size);
        MPI_Allgather(&localResultsCount,1,MPI_INT,&localResultsCounts[0],1,MPI_INT,MPI_COMM_WORLD);
        std::vector<int> localResultsDispls;
        localResultsDispls.push_back(0);
        int totalResults= localResultsCounts.at(0);
        for(int i=1;i<size;i++){
            totalResults += localResultsCounts.at(i);
            localResultsDispls.push_back(localResultsDispls.at(i-1)+localResultsCounts.at(i-1));
        }
        results.resize(totalResults);
        
        MPI_Allgatherv(&localResults[0],localResultsCount,analysisParameterDatatype,(&results[0]),&localResultsCounts[0],&localResultsDispls[0],analysisParameterDatatype,MPI_COMM_WORLD);
    }else{
        results= localResults;
    }

    double profitSum=0;
    int right=0;
    int wrong=0;
    for(ticker t:globalTickers){
        for(int i =t.clusterStart;i<t.clusterEnd+1;i++){
            if(globalDatapoints.at(i).time==testDay){
                analysisParameter temp = analysisTemplate;
                temp.base=t;
                createBaseBounds(&temp,&globalDatapoints.at(i));
            }
        }
    }
    
    
}

void safeAnalysis(analysisParameter in,std::vector<analysisParameter> *out){
        //have fully formed base before this
    
        //creates basebounds, if non preset the in->baseBoundMargin must be set. 
        //createBaseBounds(&in,&testDatapoint);//VNFE
        //I shoudl move this

        std::vector<datapoint> validBases;//certified fresh and clean, absolutely valid(So much cooler than me)
        findValidBases(&in,&validBases);
        
        //now we look through every ticker in the known universe
        //lets see if we already have bounds set though... just to make it quicker
        if(in.targetMax<in.targetMin) throw "safeAnalysis:: target bounds not set. (Why??)";
        for(ticker t: globalTickers){
            analysisParameter result=in;
            double mean = t.mean;
            double std = t.std;
            //if it is a multiday change test, the distr is diff, so reset mean and std
            if(in.targetDayDiff){
                double changeSum=0;
                double count = t.clusterEnd+1-t.clusterStart-in.targetDayDiff;
                for(int tDP =t.clusterStart+in.targetDayDiff;tDP<t.clusterEnd+1;tDP++){
                    changeSum += globalDatapoints.at(tDP).adjClose/globalDatapoints.at(tDP-in.targetDayDiff).adjClose-1;
                }
                mean = changeSum/count;
                double stdSum=0;
                for(int tDP =t.clusterStart+in.targetDayDiff;tDP<t.clusterEnd+1;tDP++){
                    double change = globalDatapoints.at(tDP).adjClose/globalDatapoints.at(tDP-in.targetDayDiff).adjClose-1;
                    stdSum += (change-mean)*(change-mean);
                }
                std = sqrt(stdSum/(double)(count));
            }

            for(datapoint bDP:validBases){
                for(int tDP =t.clusterStart;tDP<t.clusterEnd+1-in.targetDayOffset;tDP++){
                    /// work on this stuff, possible loss of data
                    if(globalDatapoints.at(tDP).time==bDP.time){
                        //now bound checking etc
                        double change = in.targetDayDiff?globalDatapoints.at(tDP+in.targetDayOffset).adjClose/globalDatapoints.at(tDP+in.targetDayOffset-in.targetDayDiff).adjClose-1:(globalDatapoints.at(tDP+in.targetDayOffset).change);
                        result.targetTotal++;
                        if(change>=in.targetMin&&change<=in.targetMax){
                            result.targetInBound++;
                        }
                    }
                }
            }

            //now we see if it is a good sample
            
            if(!result.targetTotal) continue;
            double zout = ((double)((double)result.targetInBound/(double)result.targetTotal)-result.targetPercentInc)/sqrt((1-result.targetPercentInc)*result.targetPercentInc/result.targetTotal);
            if(zout<result.targetZScore) continue;
            result.target = t;
            result.targetZScore=zout;
            ->push_back(result);
            logger(result);
        }
}
std::vector<analysisParameter> runAnalysisOnDay(int MULTITHREAD_MODE,analysisParameter analysisTemplate,std::vector<datapoint> testData){
    std::vector<analysisParameter> results;
    //logger(analysisTemplate);
    for(datapoint d:testData){for(ticker t:globalTickers){
        if(t.id==d.id){
            analysisParameter current = analysisTemplate;
            current.base=t;
            safeAnalysis(current,&results,d);
        }
    }}
    return results;
}
std::vector<analysisParameter> runAnalysisOnDay(int MULTITHREAD_MODE,analysisParameter analysisTemplate,long long testDate){
    std::vector<analysisParameter> results;
    //logger(analysisTemplate);
    for(datapoint d:testData){for(ticker t:globalTickers){
        if(t.id==d.id){
            analysisParameter current = analysisTemplate;
            current.base=t;
            safeAnalysis(current,&results,d);
        }
    }}
    return results;
}
double getProfitFromDay()

void backtestAnalysis(long long startDate, long long endDate, void analysis(analysisParameter,std::vector<analysisParameter> *,datapoint)){
    
}


template<typename T>
double backtestDailyInvestmentStrategy(int MULTITHREAD_MODE,T requestTemplate,std::vector<std::string> *results, long long startingTestDate,long long endingTestDate,double zCutoff,double dailyStrategy(int,T,std::vector<T> *,long long,double)){
    
    //the tuple tells me what ticker to look at, The range it should be in, the strategy outputs these based on likelihood
    //I need to know how to invest in the strategy. 
    //strategyParameters- these parameters are not the same per method
    /**
     * @brief The identifyStrategyResponsibiliy
     * Decides which child strategies are multithreaded, or if multithread goes on here
     */
    addHeader("identifyStrategyResponsibility");
    long long childMultithreadableStart=0;
    long long childMultithreadableEnd=0;
    long long localStart=0;
    long long localEnd=0;
    if(MULTITHREAD_MODE==MULTITHREAD_PARENT_OFF){
        long long daysTested = (endingTestDate-startingTestDate+DAY_VALUE)/DAY_VALUE;
        if(daysTested<size){
            childMultithreadableStart=startingTestDate;
            childMultithreadableEnd=endingTestDate;
        }else{
            long long partition =(endingTestDate-startingTestDate+DAY_VALUE)/size;
            localStart=partition*rank+startingTestDate;
            localEnd= partition*(rank+1)+startingTestDate;
            childMultithreadableStart=partition*(size)+startingTestDate;
            childMultithreadableEnd=endingTestDate;
        }
    }else{
        childMultithreadableStart=startingTestDate;
        childMultithreadableEnd=endingTestDate;
        MULTITHREAD_MODE=MULTITHREAD_PARENT_ON;
    }
    /* MPI_Barrier(MPI_COMM_WORLD);
    logger("locS:"+std::to_string(localStart)+" locE:"+std::to_string(localEnd)+" chilS:"+std::to_string(childMultithreadableStart)+" chilE:"+std::to_string(childMultithreadableEnd));
    MPI_Barrier(MPI_COMM_WORLD); */
    removeHeader("identifyStrategyResponsibility");

    /**
     * @brief findLocallyComputedDailyStrategyProfits
     * adds all the locally computed profits to a localprofitvector
     */
    addHeader("findLocallyComputedDailyStrategyProfits");
    std::vector<double> localProfits;
    for(long long i =localStart;i<localEnd;i+=DAY_VALUE){
        requestTemplate.timeMaxBound=i;
        std::vector<T> garbage;
        localProfits.push_back(dailyStrategy(MULTITHREAD_PARENT_ON,requestTemplate,&garbage,i,zCutoff));
    }
   /*  MPI_Barrier(MPI_COMM_WORLD);
    logger("lc size: "+std::to_string(localProfits.size()));
    MPI_Barrier(MPI_COMM_WORLD); */
    removeHeader("findLocallyComputedDailyStrategyProfits");

     /**
     * @brief combineLocalProfitVectors
     * If it is multithreaded, combines all local vectors
     * otherwise just copies local vector over
     */
    addHeader("combineLocalProfitVectors::");
    std::vector<double> profits;
    if(MULTITHREAD_MODE==MULTITHREAD_PARENT_OFF){
        int localProfitCount = localProfits.size();
        std::vector<int> localProfitCounts;
        localProfitCounts.resize(size);
        MPI_Allgather(&localProfitCount,1,MPI_INT,&localProfitCounts[0],1,MPI_INT,MPI_COMM_WORLD);
        std::vector<int> localProfitDispls;
        localProfitDispls.push_back(0);
        int totalProfits= localProfitCounts.at(0);
        for(int i=1;i<size;i++){
            totalProfits += localProfitCounts.at(i);
            localProfitDispls.push_back(localProfitDispls.at(i-1)+localProfitCounts.at(i-1));
        }
        profits.resize(totalProfits);
        
        MPI_Allgatherv(&localProfits[0],localProfitCount,MPI_DOUBLE,(&profits[0]),&localProfitCounts[0],&localProfitDispls[0],MPI_DOUBLE,MPI_COMM_WORLD);
    }else{
        profits= localProfits;
    }
    /*  MPI_Barrier(MPI_COMM_WORLD);
    logger("p size: "+std::to_string(profits.size()));
    MPI_Barrier(MPI_COMM_WORLD); */
    removeHeader("combineLocalProfitVectors::");

    

     /**
     * @brief findGloballyComputedDailyStrategyProfits
     * adds all the globally computed profits to the profit vector
     */
    addHeader("findGloballyComputedDailyStrategyProfits");
    for(long long i=childMultithreadableStart;i<=childMultithreadableEnd;i+=DAY_VALUE){
        requestTemplate.timeMaxBound=i;
        std::vector<T> garbage;
        profits.push_back(dailyStrategy(MULTITHREAD_MODE,requestTemplate,&garbage,i,zCutoff));
    }
   /* MPI_Barrier(MPI_COMM_WORLD);
    logger("fp size: "+std::to_string(profits.size()));
    MPI_Barrier(MPI_COMM_WORLD);*/
    removeHeader("findGloballyComputedDailyStrategyProfits"); 
    double profit =1.0;
    for(double p:profits){
        if(p!=0) results->push_back(std::to_string(p));
        profit*=(1+p);
    }
    return profit;
}



template<typename T>
std::vector<T> runDailyInvestmentStrategy(T requestTemplate,long long testDate,double zCutoff,double dailyStrategy(int,T,std::vector<T> *,long long,double)){
    std::vector<T> out;
    addHeader("runStrategy");
    dailyStrategy(requestTemplate,&out,testDate,zCutoff);
    removeHeader();
    return out;
}

void initialization(std::string inputFileName){
    
    std::vector<datapoint>().swap(globalDatapoints);
    std::vector<ticker>().swap(globalTickers);

    //O(1)
    addHeader("declareDatatypes::");
    declareDatatypes();
    removeHeader("initialization::Declared Datatypes");

//1535068800
    //Unknown BC IO
    addHeader("loadFileToDatapointsVector::");
    std::vector<datapoint> unClusteredDatapoints;
    if(rank ==0) loadFileToDataStructure(&inputFileName,&unClusteredDatapoints,&parseSourceCSVLineToDataPoint);
    removeHeader("initialization::Loaded File to Datapoints Vector");


    //Unknown BC MPI
    addHeader("sharingUnClusteredDatapoints::");
    int datapointCount = unClusteredDatapoints.size();
    MPI_Bcast(&datapointCount,1,MPI_INT,0,MPI_COMM_WORLD);
    if(rank!=0) unClusteredDatapoints.resize(datapointCount);
    MPI_Bcast(&unClusteredDatapoints[0],datapointCount,datapointDatatype,0,MPI_COMM_WORLD);
    removeHeader("initialization::Shared UnClustered Datapoints");

    //~O(datapointcount/cores)
    addHeader("getLocalUnClusteredDatapoints::");
    std::vector<datapoint> localUnClusteredDatapoints;
    for(int i = rank;i<datapointCount;i+=size){
        localUnClusteredDatapoints.push_back(unClusteredDatapoints.at(i));
    }
    removeHeader("initialization::Got Local UnClustered Datapoints");


    //~O(datapointcount*tickercount/cores)
    addHeader("identifyLocalUnClusteredTickers::");
    std::vector<ticker> localUnClusteredTickers;
    identifyTickersFromDatapoints(&localUnClusteredDatapoints,&localUnClusteredTickers);
    removeHeader("initialization::Identified Local UnClustered Tickers");

    //Unknown BC MPI
    addHeader("regatherUnClusteredTickers::");
    int localUnClusteredTickersCount = localUnClusteredTickers.size();
    std::vector<ticker> tickers;
    std::vector<int> unClusteredTickersCounts;
    unClusteredTickersCounts.resize(size);
    std::vector<int> unClusteredTickersDispls;
    unClusteredTickersDispls.push_back(0);
    MPI_Allgather(&localUnClusteredTickersCount,1,MPI_INT,&unClusteredTickersCounts[0],1,MPI_INT,MPI_COMM_WORLD);
    int tickerCountsSum = unClusteredTickersCounts.at(0);
    for(int i=1;i<size;i++){
        unClusteredTickersDispls.push_back(unClusteredTickersDispls.at(i-1)+unClusteredTickersCounts.at(i-1));
        tickerCountsSum += unClusteredTickersCounts.at(i);
    }
    logger(std::to_string(tickerCountsSum));
    tickers.resize(tickerCountsSum);
    MPI_Allgatherv(&localUnClusteredTickers[0],localUnClusteredTickersCount,tickerDatatype,&tickers[0],&unClusteredTickersCounts[0],&unClusteredTickersDispls[0],tickerDatatype,MPI_COMM_WORLD);
    removeHeader("initialization::Regathered UnClustered Tickers");

    //Unknown BC std::
    addHeader("deDuplicateUnClusteredTickers::");
    std::sort(
        tickers.begin(),
        tickers.end(),
        [](ticker &a,ticker &b){
            return a.id<b.id;
        }
    );
    auto last = std::unique(
        tickers.begin(), 
        tickers.end(),
        [](ticker &a,ticker &b){
            return a.id==b.id;
        }
    );
    tickers.erase(last, tickers.end());
    removeHeader();


    //~O(tickercount/cores)
    addHeader("identifyLocalTickers::");
    std::vector<ticker> localTickers;
    int commonPartition = (tickers.size()/size>0)?tickers.size()/size:1;
    for(int i =0;i<tickers.size();i++){
        if(rank==i%size) localTickers.push_back(tickers.at(i));
    }
    removeHeader();


    //The next two sections could be combined
    //IE identify and sort on one run, 
    //thing is I dont want to implemnt my own sorting alogoritm

    //~O(datapointcount*tickercount/cores)
    addHeader("iidentifyLocalDatapoints::");
    std::vector<datapoint> localDatapoints;
    for(int i=0;i<unClusteredDatapoints.size();i++){
        for(int j = 0;j<localTickers.size();j++){
            if(localTickers.at(j).id==unClusteredDatapoints.at(i).id){
                localDatapoints.push_back(unClusteredDatapoints.at(i));
                j=localTickers.size();
            }
        }
    }
    std::vector<ticker>().swap(localTickers);
    std::vector<ticker>().swap(tickers);
    removeHeader();


    //~O(?) sorting
    addHeader("sortDatapoints::");
    std::sort(
        localDatapoints.begin(),
        localDatapoints.end(),
        [](datapoint &a,datapoint &b){
            if(a.id<b.id) return true;
            if(a.id>b.id) return false;
            return a.time<b.time;
        }
    );
    removeHeader();


    //regather O(unkown)
    addHeader("regatherDatapoints::");
    int localDatapointCount = localDatapoints.size();
   
    if(rank==0) globalDatapoints.resize(datapointCount);
    std::vector<int> datapointCounts;
    datapointCounts.resize(size);
    std::vector<int> datapointDispls;
    if(rank==0) datapointDispls.push_back(0);
    MPI_Gather(&localDatapointCount,1,MPI_INT,&datapointCounts[0],1,MPI_INT,0,MPI_COMM_WORLD);
    for(int i=1;i<size;i++){
        if(rank==0) datapointDispls.push_back(datapointDispls.at(i-1)+datapointCounts.at(i-1));
    }
    MPI_Gatherv(&localDatapoints[0],localDatapointCount,datapointDatatype,&globalDatapoints[0],&datapointCounts[0],&datapointDispls[0],datapointDatatype,0,MPI_COMM_WORLD);
    std::vector<datapoint>().swap(localDatapoints);
    std::vector<datapoint>().swap(unClusteredDatapoints);
    removeHeader("");



    addHeader("generateTickerSchema::");
    if(rank==0) generateTickerSchema(&globalDatapoints,&globalTickers);
    removeHeader();
    

}

void startup(std::string datapointFileName,std::string tickerSchemaFileName){

    std::vector<datapoint>().swap(globalDatapoints);
    std::vector<ticker>().swap(globalTickers);

    logger("startup::Declaring Datatypes");
    declareDatatypes();
    logger("startup::Declared Datatypes");

    logger("startup::Loading File to Datapoints Vector");
    if(rank==0) loadFileToDataStructure(&datapointFileName,&globalDatapoints,&parseStorageLineToDataPoint);
    logger("startup::Loaded File to Datapoints Vector");

    logger("startup::Loading File ticker schemas Vector");
    if(rank==0) loadFileToDataStructure(&tickerSchemaFileName,&globalTickers,&parseStorageLineToTicker);
    logger("startup::Loaded File to ticker schemas Vector");


    logger("startup::sharing vectors");
    shareDatapoints();
    logger("startup::shared vectors");
}

void pruneGlobalVectorsToGivenLength(int lines){

    logger("pruneGlobalVectorsToGivenLength::Resizing Datapoints Vector");
    globalDatapoints.resize(lines);
    logger("pruneGlobalVectorsToGivenLength::Resized Datapoints Vector");

    logger("pruneGlobalVectorsToGivenLength::Recreating Ticker Schema");
    std::vector<ticker> newTickers;
    for(int i=0;i<globalTickers.size();i++){
        if(globalTickers.at(i).clusterEnd>=lines){
            globalTickers.at(i).clusterEnd=lines-1;
            double meanSum=0;
            for(int j = globalTickers.at(i).clusterStart;j<globalTickers.at(i).clusterEnd;j++) meanSum += globalDatapoints.at(j).change;
            globalTickers.at(i).mean = meanSum/globalTickers.at(i).clusterEnd-globalTickers.at(i).clusterStart;
            double stdSum=0;
            for(int j = globalTickers.at(i).clusterStart;j<globalTickers.at(i).clusterEnd;j++){
                stdSum += (globalDatapoints.at(j).change-globalTickers.at(i).mean)*(globalDatapoints.at(j).change-globalTickers.at(i).mean);
            
            }
            globalTickers.at(i).std= sqrt(stdSum/(double)(lines-globalTickers.at(i).clusterStart));

            break;
        }else{
            newTickers.push_back(globalTickers.at(i));
        }
    }
    globalTickers.swap(newTickers);
    logger("pruneGlobalVectorsToGivenLength::Recreated Ticker Schema");

}

void filterGlobalVectorsByGivenTickers(std::vector<std::string> filterTickers){

    addHeader("populateNewDatapointsAndTickerVectors::");
    std::vector<datapoint> newDatapoints;
    std::vector<ticker> newTickers;
    for(std::string currentFilterTicker:filterTickers){
        int myID=0;
        int charsToPushFromTickerName = currentFilterTicker.size()>TICKER_NAME_LENGTH?TICKER_NAME_LENGTH:currentFilterTicker.size();
        if(charsToPushFromTickerName){
            char name[TICKER_NAME_LENGTH]={' '};
            for(int i=0;i<charsToPushFromTickerName;i++) name[i] = currentFilterTicker.at(i);
            for(int i =0;i<TICKER_NAME_LENGTH;i++) myID |= (name[i] << (i*8));
        }
        if(myID){
            for(ticker currentTicker:globalTickers){
                if(currentTicker.id==myID){
                    ticker newTicker = currentTicker;
                    newTicker.clusterStart=newDatapoints.size();
                    for(int i = currentTicker.clusterStart;i<currentTicker.clusterEnd+1;i++){
                        newDatapoints.push_back(globalDatapoints.at(i));
                    }
                    newTicker.clusterEnd=newDatapoints.size()-1;
                    newTickers.push_back(newTicker);
                    break;
                }
            }
        }
    }
    removeHeader();

    addHeader("setGlobalVectorsToNewVectors::");
    globalDatapoints.swap(newDatapoints);
    globalTickers.swap(newTickers);
    removeHeader();

}

void writeGlobalVectorsToFiles(std::string datapointFileName,std::string tickerFileName){

    addHeader("loadingDatapointsVectorToFile::");
    loadDatastructsToFile(&datapointFileName,&globalDatapoints,&parseDataPointToStorageLine);
    removeHeader();

    addHeader("loadingTickerSchemasVectorToFile::");
    loadDatastructsToFile(&tickerFileName,&globalTickers,&parseTickerToStorageLine);
    removeHeader();

}





int main(int argc,char** argv){

    MPI_Init(&argc,&argv);

    MPI_Comm_rank(MPI_COMM_WORLD,&rank);
    MPI_Comm_size(MPI_COMM_WORLD,&size);
    /**
    addHeader("initialization::");
    initialization("../assets/historical_daily_data_kaggle");
    removeHeader();

    addHeader("writeInitVectorsToFiles::");
    writeGlobalVectorsToFiles("../assets/datapointdeepstorage","../assets/tickerschemadeepstorage");
    removeHeader();

     addHeader("filterInitVectors::");
    std::vector<std::string> in = {"KR","PG","AAPL","KO","MSFT"};
    filterGlobalVectorsByGivenTickers(in);
    removeHeader();

    addHeader("writeFilteredVectorsToFiles::");
    writeGlobalVectorsToFiles("../assets/KRGEdeepstorage","../assets/KRGESchemadeepstorage");
    removeHeader();
**/


    //run this when you get home
    addHeader("startup::");
    startup("../assets/KRGEdeepstorage","../assets/KRGESchemadeepstorage");
    //startup("../assets/datapointdeepstorage","../assets/tickerschemadeepstorage");
    removeHeader();

    /**
    addHeader("filterInitVectors::");
    std::vector<std::string> in = {"AAPL","AMZN","FB","TSLA","F","T","VZ","F","GPRO","KO","MSFT"};
    filterGlobalVectorsByGivenTickers(in);
    removeHeader();

    addHeader("writeFilteredVectorsToFiles::");
    writeGlobalVectorsToFiles("../assets/KRGEdeepstorage","../assets/KRGESchemadeepstorage");
    removeHeader();
    **/
    MPI_Barrier(MPI_COMM_WORLD);
    logger("to dps");
    MPI_Barrier(MPI_COMM_WORLD);

    std::vector<datapoint> testDps;
    datapoint dp1=datapoint{
            .id=convertNameToID("AAPL"),
            .name="AAPL",
            .open=146.28,
            .close=149.84,
            .adjClose=149.84,
            .change=-.0129
        };
    datapoint dp2 = datapoint{
            .id=convertNameToID("AMZN"),
            .name="AMZN",
            .time=0,
            .open=114.03,
            .close=118.05,
            .adjClose=118.05,
            .change=.0312
    };
    testDps.push_back(dp1);
    testDps.push_back(dp2);
    
    MPI_Barrier(MPI_COMM_WORLD);
    logger("dps created");
    MPI_Barrier(MPI_COMM_WORLD);
    analysisParameter testAP = analysisParameter
    {
    .baseMin=__DBL_MAX__,
    .baseMax=__DBL_MIN__,
    .baseBoundMargin=.05,
    .targetMin=0,
    .targetMax=1,
    .targetZScore=0,
    .targetPercentInc=.6,
    .baseDayDiff=0,
    .targetDayDiff=2,
    .timeMin=LONG_LONG_MIN,
    .timeMax=LONG_LONG_MAX,
    .targetDayOffset=2
    };
    MPI_Barrier(MPI_COMM_WORLD);
    logger("ap created");
    MPI_Barrier(MPI_COMM_WORLD);
    std::vector<analysisParameter> out;
    addHeader("runAnalysis");
    if(rank==0) out = runAnalysisOnDay(0,testAP,testDps);
    removeHeader();
    /**}
    catch (const char* msg) {
     std::cerr << msg << std::endl;
   }**/

   
    //double normalProfit = backtestDailyInvestmentStrategy(MULTITHREAD_PARENT_OFF,strat,&garbage,globalDatapoints.at(globalDatapoints.size()-150).time,globalDatapoints.at(globalDatapoints.size()-100).time,0,&longTermInvestmentStrategy);
    

   

    addHeader("writeResultsTofile");
  
    std::string fileNameProfit= "../assets/results";
    loadDatastructsToFile(&fileNameProfit,&out,&parseAnalysisParameterToStorageLine);
    removeHeader();


    
    //startup("../assets/datapointdeepstorage","../assets/tickerschemadeepstorage");
    
/**
        double out = (backtestDailyInvestmentStrategy(strat,&results,clusteredDatapoints.at(clusteredDatapoints.size()-2000).time,clusteredDatapoints.at(clusteredDatapoints.size()-1).time,1.64,&dayAfterIncStrategy));
        std::cout<<std::to_string(out)<<"\n";
        if(rank==0){
            std::string toshow = std::to_string(i)+";"+std::to_string(clusteredDatapoints.at(clusteredDatapoints.size()-i).time)+";"+std::to_string(out);
             std::cout<<toshow<<"\n";
             results.push_back(toshow);
        }
    }

    std::string fileName= "../assets/results";
    loadDatastructsToFile(&fileName,&results,&parseStringToStorageLine);**/
    
   

    //startup("../assets/datapointdeepstorage","../assets/tickerschemadeepstorage");
    
    

    MPI_Finalize();

}
////////////////////////////////////////////////////////////////////////////////////////////////////