
#include <stdio.h>
#include <stddef.h>
#include <string.h>
#include <vector>
#include <time.h>
#include <ctime>
#include <iomanip>
#include <fstream>
#include <iostream>
#include <math.h>
#include <algorithm>
#include <sys/time.h>
#include <mpi.h>


//Constants and Definintions
////////////////////////////////////////////////////////////////////////////////////////////////////
const int TICKER_NAME_LENGTH =8;
const int LINE_LOG_LENGTH=100000;
struct datapoint{
    unsigned long long id=0;
    char name[TICKER_NAME_LENGTH]={' '};
    long long time;
    double open;
    double close;
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
struct report{
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
MPI_Datatype reportDatatype;
////////////////////////////////////////////////////////////////////////////////////////////////////


//FIX, not efficient
//Globals
////////////////////////////////////////////////////////////////////////////////////////////////////
std::vector<ticker> finalTickerSchema;
std::vector<datapoint> clusteredDatapoints;
int rank;
int size;
////////////////////////////////////////////////////////////////////////////////////////////////////




//Logging
////////////////////////////////////////////////////////////////////////////////////////////////////
#define LOG 1
#define TIME 1
std::vector<std::string> log_Header_Vector;
std::string log_Header;
/**
 * @brief For implicit compile time logging
 * 
 * @param message 
 */
void log(std::string message){
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
    
	std::string output= "NODE: "+std::to_string(100+rank).substr(1,2)+timestamp+" LOG: "+message+"\n";
	std::cout<<output;
	#endif
}

void addHeader(std::string in){
    log_Header_Vector.push_back(in);
    log_Header += in;
}

void removeHeader(){
    log_Header_Vector.pop_back();
    for(std::string log_Header_Val:log_Header_Vector) log_Header += log_Header_Val;
}
////////////////////////////////////////////////////////////////////////////////////////////////////


//File IO
////////////////////////////////////////////////////////////////////////////////////////////////////
/**
 * @brief Takes a SourceCSV-type string [line] and converts it to a datapoint[in] based on [delimiter]
 * 
 * @param out datapoint out
 * @param line string line in
 * @param delimiter string delimiter
 * @return true std::string line successfully parsed to datapoint *out
 * @return false std::string line could not be parsed to datapoint *out
 */
bool parseSourceCSVLineToDataPoint(datapoint *out,std::string line,std::string delimiter){
	std::string name;
	struct std::tm tm ={0};
    long long time;
	double open=0;
	double close=0;
	double change=0;
	try{
    //ticker,open,close,aclose,low,high,volume,date
	name = line.substr(0,line.find(delimiter));line.erase(0,line.find(delimiter)+1);//ticker
    open = stod(line.substr(0,line.find(delimiter)));line.erase(0,line.find(delimiter)+1);//open
    line.erase(0,line.find(delimiter)+1);//close
    close = stod(line.substr(0,line.find(delimiter)));line.erase(0,line.find(delimiter)+1);//adjclose
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
			out->change = (close/open)-1;
            out->time = time;
            return true;
	}else return false;
}
/**
 * @brief Takes a Storage-type string [line] and converts it to a datapoint[in] based on [delimiter]
 * 
 * @param out datapoint out
 * @param line string line in
 * @param delimiter string delimiter
 * @return true std::string line successfully parsed to datapoint *out
 * @return false std::string line could not be parsed to datapoint *out
 */
bool parseStorageLineToDataPoint(datapoint *out,std::string line,std::string delimiter){
    
    unsigned long long id=0;
    std::string name;
    long long time;
    struct std::tm tm ={0};
    double open=0;
    double close=0;
    double change=0;

	try{
    //id,name,time,open,close,change
    id = stoll(line.substr(0,line.find(delimiter)));line.erase(0,line.find(delimiter)+1);//id
	name = line.substr(0,line.find(delimiter));line.erase(0,line.find(delimiter)+1);//name
    time = stoll(line.substr(0,line.find(delimiter)));line.erase(0,line.find(delimiter)+1);//time
    open = stod(line.substr(0,line.find(delimiter)));line.erase(0,line.find(delimiter)+1);//open
    close = stod(line.substr(0,line.find(delimiter)));line.erase(0,line.find(delimiter)+1);//close
    change = stod(line);
	}catch(std::exception e){}
	
	int charsToPushFromName = name.size()>TICKER_NAME_LENGTH?TICKER_NAME_LENGTH:name.size();
    if(charsToPushFromName>0){
			for(int i=0;i<charsToPushFromName;i++)
				out->name[i] = name.at(i);
            out->id = id;
			out->close = close;
			out->open = open;
			out->change = (close/open)-1;
            out->time = time;
			return true;
	}else return false;

}
/**
 * @brief Takes a Storage-type string [line] and converts it to a ticker[in] based on [delimiter]
 * 
 * @param out ticker out
 * @param line string line in
 * @param delimiter string delimiter
 * @return true std::string line successfully parsed to datapoint *out
 * @return false std::string line could not be parsed to datapoint *out
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
 * 
 * @param in datapoint in
 * @param delimiter the string delimiter
 * @return std::string output line
 */
void parseDataPointToStorageLine(datapoint *in,std::string delimiter,std::string *out){
    out->append(std::to_string(in->id)+delimiter+(std::string)in->name+delimiter+std::to_string(in->time)+delimiter+std::to_string(in->open)+delimiter+std::to_string(in->close)+delimiter+std::to_string(in->change)+"\n");
}
/**
 * @brief Takes a ticker[in] and converts it to a Storage-type string [line] with [delimiter]
 * 
 * @param in ticker in
 * @param delimiter the string delimiter
 * @return std::string output line
 */
void parseTickerToStorageLine(ticker *in,std::string delimiter,std::string *out){
    out->append(std::to_string(in->id)+delimiter+std::to_string(in->clusterStart)+delimiter+std::to_string(in->clusterEnd)+delimiter+std::to_string(in->mean)+delimiter+std::to_string(in->std)+"\n");
}
/**
 * @brief Takes a report[in] and converts it to a Storage-type string [line] with [delimiter]
 * 
 * @param in report in
 * @param delimiter the string delimiter
 * @return std::string output line
 */
void parseReportToStorageLine(report *in,std::string delimiter,std::string *out){
    out->append(std::to_string(in->base.id)+delimiter+std::to_string(in->target.id)+delimiter+std::to_string(in->baseMinBound)+delimiter+std::to_string(in->baseMaxBound)+delimiter+std::to_string(in->targetMinBound)+delimiter+std::to_string(in->targetMaxBound)+delimiter+std::to_string(in->timeMinBound)+delimiter+std::to_string(in->timeMaxBound)+delimiter+std::to_string(in->totalTarget)+delimiter+std::to_string(in->totalTargetInBound)+delimiter+std::to_string(in->totalBase)+delimiter+std::to_string(in->totalBaseInBound)+delimiter+std::to_string(in->dayDifference)+delimiter+std::to_string(in->likelihood)+delimiter+std::to_string(in->zScore)+delimiter+std::to_string(in->responseVal)+"\n");
}
/**
 * @brief Loads file of name [fileName] to a vector of data type T[output]
 * 
 * @param fileName string name of file to parse
 * @param output pointer to datapoints vector to output data to
 */
template<typename T>
void loadFileToDataStructure(std::string *fileName,std::vector<T> *output,bool lineParseFunction(T *,std::string,std::string)){
    std::ifstream myFile;
	if(rank==0) log("loadFileToDataStructure::Opening file <"+*fileName+">");
	myFile.open(*fileName);
	if(rank==0) log("loadFileToDataStructure::File <"+*fileName+"> opened");
	std::string line;
    getline(myFile, line);
	#if LOG
	int linesParsed=0; 
	#endif
	while(getline(myFile, line))
    {
        T temp;
        bool readable = lineParseFunction(&temp,line,",");
        if(readable) output->push_back(temp);
        
		#if LOG
        if(!readable) log("loadFileToDataStructure::PARSE FAILURE on <"+line+">");
		if(readable) linesParsed++;
		if(linesParsed%LINE_LOG_LENGTH==0) log("loadFileToDataStructure::Parsed <"+std::to_string(linesParsed)+"> Lines");
		#endif
    }
    #if LOG
    if(rank==0) log("loadFileToDataStructure:: <"+std::to_string(linesParsed)+"> Lines Parsed");
    #endif
    myFile.close();
}
/**
 * @brief Loads vector of type T[input] to file of name [fileName]
 * 
 * @param fileName name of file to write to
 * @param output pointer to datapoints vector
 * 
 */
template<typename T>
void loadDatastructsToFile(std::string *fileName,std::vector<T> *input,void datastructParseFunction(T *,std::string,std::string *)){
	if(rank==0) log("loadDatastructsToFile::Opening file <"+*fileName+">");
    MPI_File myFile;
    char fntmp[fileName->length()];
    strcpy(fntmp,fileName->c_str());
    MPI_Barrier(MPI_COMM_WORLD);
    MPI_File_delete(fntmp,MPI_INFO_NULL);
    MPI_File_open(MPI_COMM_WORLD,fntmp,MPI_MODE_RDWR|MPI_MODE_CREATE,MPI_INFO_NULL,&myFile);
    
	if(rank==0) log("loadDatastructsToFile::File <"+*fileName+"> opened");
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
		if(linesWritten%LINE_LOG_LENGTH==0) log("loadDatastructsToFile::wrote <"+std::to_string(linesWritten)+"> Lines");
		#endif
    }}
    #if LOG
   if(rank==0) log("loadDatastructsToFile:: <"+std::to_string(linesWritten)+"> Lines Written");
    #endif
    
    MPI_File_close(&myFile);
}
////////////////////////////////////////////////////////////////////////////////////////////////////



//Backside Functions
////////////////////////////////////////////////////////////////////////////////////////////////////
void declareDatatypes(){

	int lengths[] = {1,TICKER_NAME_LENGTH,1,1,1,1};
	MPI_Aint disps[] = {offsetof(datapoint,id),offsetof(datapoint,name),offsetof(datapoint,time),offsetof(datapoint,open),offsetof(datapoint,close),offsetof(datapoint,change)};
	MPI_Datatype types[] = {MPI_LONG_LONG,MPI_CHAR,MPI_LONG_LONG,MPI_DOUBLE,MPI_DOUBLE,MPI_DOUBLE};
	MPI_Type_create_struct(6,lengths,disps,types,&datapointDatatype);
	MPI_Type_commit(&datapointDatatype);

    int tlengths[] = {1,1,1,1,1};
	MPI_Aint tdisps[] = {offsetof(ticker,id),offsetof(ticker,clusterStart),offsetof(ticker,clusterEnd),offsetof(ticker,mean),offsetof(ticker,std)};
	MPI_Datatype ttypes[] = {MPI_LONG_LONG,MPI_INT,MPI_INT,MPI_DOUBLE,MPI_DOUBLE};
	MPI_Type_create_struct(5,tlengths,tdisps,ttypes,&tickerDatatype);
	MPI_Type_commit(&tickerDatatype);

    int rlengths[] = {1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1};
	MPI_Aint rdisps[] = {offsetof(report,base),offsetof(report,target),offsetof(report,baseMinBound),offsetof(report,baseMaxBound),offsetof(report,targetMinBound),offsetof(report,targetMaxBound),offsetof(report,timeMinBound),offsetof(report,timeMaxBound),offsetof(report,totalTarget),offsetof(report,totalTargetInBound),offsetof(report,totalBase),offsetof(report,totalBaseInBound),offsetof(report,likelihood),offsetof(report,zScore),offsetof(report,dayDifference),offsetof(report,responseVal)};
	MPI_Datatype rtypes[] = {tickerDatatype,tickerDatatype,MPI_DOUBLE,MPI_DOUBLE,MPI_DOUBLE,MPI_DOUBLE,MPI_LONG_LONG,MPI_LONG_LONG,MPI_INT,MPI_INT,MPI_INT,MPI_INT,MPI_DOUBLE,MPI_DOUBLE,MPI_INT,MPI_DOUBLE};
	MPI_Type_create_struct(16,rlengths,rdisps,rtypes,&reportDatatype);
	MPI_Type_commit(&reportDatatype);

}
void shareDatapoints(){
    int finalTickerCount = finalTickerSchema.size();
    MPI_Bcast(&finalTickerCount,1,MPI_INT,0,MPI_COMM_WORLD);
    int finalDatapointCount = clusteredDatapoints.size();
    MPI_Bcast(&finalDatapointCount,1,MPI_INT,0,MPI_COMM_WORLD);
    if(rank>0) finalTickerSchema.resize(finalTickerCount);
    MPI_Bcast(finalTickerSchema.data(),finalTickerCount,tickerDatatype,0,MPI_COMM_WORLD);
    if(rank>0) clusteredDatapoints.resize(finalDatapointCount);
    MPI_Bcast(&(clusteredDatapoints[0]),finalDatapointCount,datapointDatatype,0,MPI_COMM_WORLD);
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
		    if(tickersIdentified%100==0) log("identifyTickersFromDatapoints:: Identified <"+std::to_string(out->size())+"> Tickers");
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
            log(std::to_string(i)+":::"+std::to_string(sum)+"::"+std::to_string(stdSum)+"::"+std::to_string(currentTicker.clusterEnd-currentTicker.clusterStart+1)+"::"+std::to_string(currentTicker.mean)+"::"+std::to_string(currentTicker.std));
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
void generateReports(report *in,std::vector<report> *out,double zscoreCutoff){
    std::vector<datapoint> baseInBound;
    //log(std::to_string(in->baseMinBound)+"::"+std::to_string(in->baseMaxBound));
    for(int i=in->base.clusterStart;i<in->base.clusterEnd+1;i++){
        if(clusteredDatapoints.at(i).change<=in->baseMaxBound&&clusteredDatapoints.at(i).change>=in->baseMinBound&&clusteredDatapoints.at(i).time<=in->timeMaxBound&&clusteredDatapoints.at(i).time>=in->timeMinBound){
           baseInBound.push_back(clusteredDatapoints.at(i));
        }
    }
    //log("base in bound <"+std::to_string(baseInBound.size()));
    in->totalBase = in->base.clusterEnd-in->base.clusterStart +1;
    in->totalBaseInBound = baseInBound.size();
    for(ticker tickerTarget:finalTickerSchema){
        if(tickerTarget.id==in->base.id) break;
        int targetStart = tickerTarget.clusterStart;
        int targetEnd = tickerTarget.clusterEnd+1;
        int totalTarget=0;
        int totalTargetInBound=0;
      
        for(datapoint base:baseInBound){
            for(int targetID = targetStart;targetID<targetEnd-in->dayDifference;targetID++){
                if(base.time==clusteredDatapoints.at(targetID).time){
                    targetStart=targetID+1;
                    totalTarget++;
                    if(clusteredDatapoints.at(targetID+in->dayDifference).change>=in->targetMinBound&&clusteredDatapoints.at(targetID+in->dayDifference).change<=in->targetMinBound){
                        totalTargetInBound++;
                    }
                    targetID=targetEnd;
                }
            }
        }
        if(totalTarget==0) continue;
        double zout = ((double)((double)totalTargetInBound/(double)totalTarget)-in->likelihood)/sqrt((1-in->likelihood)*in->likelihood/totalTarget);
        //log("zscore"+std::to_string(zout));
        if(zout>zscoreCutoff){
            //log("generateReports:: Base <"+(std::string)baseInBound.at(0).name+"> Target <"+(std::string)clusteredDatapoints.at(tickerTarget.clusterStart).name+"> InBound <"+std::to_string(totalTargetInBound)+"> Total <"+std::to_string(totalTarget)+"> Z <"+std::to_string(zout)+">");
            report newReport = *in;
            newReport.target = tickerTarget;
            newReport.totalTarget=totalTarget;
            newReport.totalTargetInBound=totalTargetInBound;
            newReport.zScore=zout;
            out->push_back(newReport);
        }
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
////////////////////////////////////////////////////////////////////////////////////////////////////




void backtestDailyStrategy(report requestTemplate,long long startingTestDate,long longEndingTestDate,double dailyStrategy(report requestTemplate,long long testDate)){

}


double dayAfterIncStrategy(report requestTemplate,long long testDate,double zCutoff){
    double requestMargin = .05;
    
    addHeader("dayAfterIncStrategy::");
    long long previousDayDate =0;
    bool found=false;
    std::vector<report> requests;
    for(ticker t:finalTickerSchema){
        for(int i=t.clusterStart+requestTemplate.dayDifference;i<t.clusterEnd+1;i++){
            if(clusteredDatapoints.at(i).time==testDate){
                report newRequest = requestTemplate;
                double percentile = getPercentile((clusteredDatapoints.at(i-requestTemplate.dayDifference).change-t.mean)/t.std);
                newRequest.baseMinBound= getZScore((percentile - requestMargin)<0?.01:(percentile - requestMargin))*t.std+t.mean;
                newRequest.baseMaxBound = getZScore((percentile + requestMargin)>1?.99:(percentile + requestMargin))*t.std+t.mean;
                newRequest.base=t;
                requests.push_back(newRequest);
                break;
            }
        }
    }

    std::vector<report> localResponses;
    int defaultPartition = (requests.size()<size)?1:requests.size()/size;
    int end =  (requests.size()<size)||(rank+1==size)?requests.size():(rank+1)*defaultPartition;
    for(int i = rank*defaultPartition;i<end;i++){
        std::vector<datapoint> basesInBound;
        requests.at(i).totalBase= requests.at(i).base.clusterEnd+1-requests.at(i).base.clusterStart;
        for(int baseID = requests.at(i).base.clusterStart; baseID<requests.at(i).base.clusterEnd+1;baseID++){
            if(clusteredDatapoints.at(baseID).time>=requests.at(i).timeMinBound
             &&clusteredDatapoints.at(baseID).time<=requests.at(i).timeMaxBound
             &&clusteredDatapoints.at(baseID).change>=requests.at(i).baseMinBound
             &&clusteredDatapoints.at(baseID).change<=requests.at(i).baseMaxBound 
            ){
                basesInBound.push_back(clusteredDatapoints.at(baseID));
            }
        }
        requests.at(i).totalBaseInBound=basesInBound.size();

        for(ticker tt:finalTickerSchema){
            int ttStart = tt.clusterStart;
            report newResponse = requests.at(i);
            for(datapoint bd:basesInBound){
                for(int td=ttStart;td=tt.clusterEnd;td++){
                    if(clusteredDatapoints.at(td).time==bd.time){
                        if(clusteredDatapoints.at(td+1).time>requests.at(i).targetMaxBound) break;
                        newResponse.totalTarget++;
                        if(clusteredDatapoints.at(td).change>=requests.at(i).baseMinBound
                         &&clusteredDatapoints.at(td).change<=requests.at(i).baseMaxBound
                        ){
                            newResponse.totalTargetInBound++;
                        }
                        ttStart=td+1;
                        break;
                    }
                }
            }
            if(!requests.at(i).totalTarget) continue;
            double zout = ((double)((double)requests.at(i).totalTargetInBound/(double)requests.at(i).totalTarget)-requests.at(i).likelihood)/sqrt((1-requests.at(i).likelihood)*requests.at(i).likelihood/requests.at(i).totalTarget);
            if(zout<zCutoff) continue;
            newResponse.target = tt;
            newResponse.zScore=zout;
            localResponses.push_back(newResponse);
        }
    }

    



    if(!found){
        log("~~~ERROR::No times found with given testDate~~~");
        return 0; 
    } 
    for(ticker t:finalTickerSchema){
        for(int i=t.clusterStart;i<t.clusterEnd+1;i++){

        }
    }


    removeHeader();
}
    

//User Functions/Processes
////////////////////////////////////////////////////////////////////////////////////////////////////
double backtestStrategyOnTickers(std::vector<ticker> *in,long long timeTestStart,long long timeMinBound,long long timeMaxBound,double zscoreCutoff,double targetMinBound, double targetMaxBound,int dayDifference,double requestMargin,double likelihood){
    
    log("backtestStrategyOnTickers::Generating Report Requests");
    //O(datapointcount) time
    std::vector<report> initialReportRequests;
    for(ticker t:*in){
        for(int i=t.clusterStart;i<t.clusterEnd+1;i++){
            if(clusteredDatapoints.at(i).time>=timeTestStart&&clusteredDatapoints.at(i).time<=timeMaxBound){
                report temp;
                temp.base=t;
                temp.dayDifference=dayDifference;
                temp.timeMinBound=timeMinBound;
                temp.timeMaxBound=clusteredDatapoints.at(i).time;
                temp.targetMinBound=targetMinBound;
                temp.targetMaxBound=targetMaxBound;
                temp.likelihood=likelihood;
                double percentile = getPercentile((clusteredDatapoints.at(i).change-temp.base.mean)/temp.base.std);
                temp.baseMinBound= getZScore((percentile - requestMargin)<0?.01:(percentile - requestMargin))*temp.base.std+temp.base.mean;
                temp.baseMaxBound = getZScore((percentile + requestMargin)>1?.99:(percentile + requestMargin))*temp.base.std+temp.base.mean;
                initialReportRequests.push_back(temp);
            }
        }
    }
    log("backtestStrategyOnTickers::Generated Report Requests");  

    MPI_Barrier(MPI_COMM_WORLD);
    
    log("backtestStrategyOnTicker::Generating Report Responses");//O(datapointcount^2/size)
    std::vector<report> localReportResponses;
    int defaultPartition = (initialReportRequests.size()<size)?1:initialReportRequests.size()/size;
    int end =  (initialReportRequests.size()<size)||(rank+1==size)?initialReportRequests.size():(rank+1)*defaultPartition;
    for(int i = rank*defaultPartition;i<end;i++){
        generateReports(&initialReportRequests.at(i),&localReportResponses,zscoreCutoff);
        log("Report Request Number <"+std::to_string(i)+"> processed");
    }
    log("backtestStrategyOnTicker::Generated Report Responses");

    MPI_Barrier(MPI_COMM_WORLD);

    log("backtestStrategy::Gathering Reports");
    int localReportCount = localReportResponses.size();
    std::vector<int> localReportCounts;
    localReportCounts.resize(size);
    MPI_Gather(&localReportCount,1,MPI_INT,&localReportCounts[0],1,MPI_INT,0,MPI_COMM_WORLD);
    std::vector<int> localReportDispls;
    localReportDispls.push_back(0);
    int totalReports= rank==0?localReportCounts.at(0):0;
    for(int i=1;i<size;i++){
        totalReports += localReportCounts.at(i);
        localReportDispls.push_back(localReportDispls.at(i-1)+localReportCounts.at(i-1));
    }
    std::vector<report> reportResponses;
    reportResponses.resize(totalReports);
    MPI_Gatherv(&localReportResponses[0],localReportCount,reportDatatype,&reportResponses[0],&localReportCounts[0],&localReportDispls[0],reportDatatype,0,MPI_COMM_WORLD);
    log("backtestStrategy::Gathered Reports");
 
    MPI_Barrier(MPI_COMM_WORLD);
    if(rank==0) log("reports <"+std::to_string(reportResponses.size())+"> generated");
    MPI_Barrier(MPI_COMM_WORLD);
     
    log("backtestStrategy::Computing profitability"); 
    double allTickerSum =0;
    int allTickerCount=0;
    long long currentTime=0;
    long long currentTicker=0;
    double singleTimeSum =0;
    int singleTimeCount=1;
    double singleTickerProduct=1;
    for(report r:reportResponses){
        double change=0;
        for(int i=r.target.clusterStart;i<r.target.clusterEnd+1-dayDifference;i++){
            if(clusteredDatapoints.at(i).time==r.timeMaxBound){
                change= (clusteredDatapoints.at(i+dayDifference).change);
                r.responseVal =change;
                //log("backtestStrategy::Report <"+(std::string)clusteredDatapoints.at(r.base.clusterStart).name+"> on <"+(std::string)clusteredDatapoints.at(r.target.clusterStart).name+"> sumchange <"+std::to_string(clusteredDatapoints.at(i+dayDifference).change)+"> added Currently at <"+std::to_string(change));
            }
        }
        
        /**
         * @brief 
         * ticker1
         *  timea
         *   r1=.01 //allTickerSum =0;allTickerCount=1; currentTime=timea;currentTicker=ticker1;singleTimeSum =.01;singleTimeCount=1;singleTickerProduct=1;
         *   r2=.01 //allTickerSum =0;allTickerCount=1; currentTime=timea;currentTicker=ticker1;singleTimeSum =.02;singleTimeCount=2;singleTickerProduct=1;
         *  timeb
         *   r1=.01 //allTickerSum =0;allTickerCount=1; currentTime=timeb;currentTicker=ticker1;singleTimeSum =.01;singleTimeCount=1;singleTickerProduct=1.01;
         *   r2=.01 //allTickerSum =0;allTickerCount=1; currentTime=timeb;currentTicker=ticker1;singleTimeSum =.02;singleTimeCount=2;singleTickerProduct=1.01;
         * ticker2
         *  timea
         *   r1=.01 //allTickerSum =1.0201;allTickerCount=2; currentTime=timea;currentTicker=ticker2;singleTimeSum =.01;singleTimeCount=1;singleTickerProduct=1;
         *   r2=.01
         *  timeb
         *   r1=.01
         *   r2=.01 //allTickerSum =1.201;allTickerCount=2; currentTime=timeb;currentTicker=ticker2;singleTimeSum =.02;singleTimeCount=2;singleTickerProduct=1.01;
         */
        
        if(r.base.id==currentTicker){
            if(r.timeMaxBound==currentTime){
                singleTimeSum +=change;
                singleTimeCount++;
            }else{
                singleTickerProduct *= (1+singleTimeSum/(double)singleTimeCount);
                //log("Single Time Count <"+std::to_string(singleTimeCount)+"> single time change <"+std::to_string((1+singleTimeSum/(double)singleTimeCount))+">");
                currentTime=r.targetMaxBound;
                singleTimeSum = change;
                singleTimeCount=1;
            }
        }else{
            singleTickerProduct *= (1+singleTimeSum/(double)singleTimeCount);
            allTickerSum += singleTickerProduct-1;
            log("Prev Ticker Product <"+std::to_string(singleTickerProduct)+"> current allticker sum <"+std::to_string(allTickerSum)+">");
            allTickerCount++;
            singleTickerProduct=1;
            singleTimeSum = change;
            singleTimeCount=1;
            currentTime=r.targetMaxBound;
            currentTicker=r.base.id;
        }
    }
    singleTickerProduct *= (1+singleTimeSum/(double)singleTimeCount);
    allTickerSum += singleTickerProduct-1;
    double output = allTickerSum/allTickerCount;

    if(rank==0) log("Prev Ticker Product <"+std::to_string(singleTickerProduct)+"> current allticker sum <"+std::to_string(allTickerSum)+">");
    log("backtestStrategy::Computed profitability");

    log("backtestStrategy::Saving Backtests to File");
    std::string filename = "../assets/reports";
    loadDatastructsToFile(&filename,&reportResponses,&parseReportToStorageLine);
    log("backtestStrategy::Saved Backtests to File");
    MPI_Barrier(MPI_COMM_WORLD);
    return output;

}
/**
double backtestStrategy(std::vector<ticker> *tickersToTest,long long timeMinBound, long long timeTestEnd,double zscoreCutoff,double targetMinBound, double targetMaxBound,int dayDifference,double requestMargin,double likelihood,std::string saveTo){
    

    //O(datapointCount)
    log("backtestStrategy::Generating datapoint report requests from day");
    std::vector<report> initialReportRequests;
    for(ticker t:*tickersToTest) {
        for(int i=t.clusterStart;i<t.clusterEnd+1;i++){
            if(clusteredDatapoints.at(i).time==timeTestEnd){
                report temp;
                temp.base=t;
                temp.dayDifference=dayDifference;
                temp.timeMinBound=timeMinBound;
                temp.timeMaxBound=timeTestEnd;
                temp.targetMinBound=targetMinBound;
                temp.targetMaxBound=targetMaxBound;
                temp.likelihood=likelihood;
                double percentile = getPercentile((clusteredDatapoints.at(i).change-temp.base.mean)/temp.base.std);
                temp.baseMinBound= getZScore((percentile - requestMargin)<0?.01:(percentile - requestMargin))*temp.base.std+temp.base.mean;
                temp.baseMaxBound = getZScore((percentile + requestMargin)>1?.99:(percentile + requestMargin))*temp.base.std+temp.base.mean;
                initialReportRequests.push_back(temp);
            }
        }
    }
        
    log("backtestStrategy::Generated datapoint report requests from day");
    MPI_Barrier(MPI_COMM_WORLD);

    

    MPI_Barrier(MPI_COMM_WORLD);

    
    
    MPI_Barrier(MPI_COMM_WORLD);
    if(rank==0) log("reports <"+std::to_string(reportResponses.size())+"> generated");
    MPI_Barrier(MPI_COMM_WORLD);

    log("backtestStrategy::Saving Backtests to File");
    loadDatastructsToFile(&saveTo,&reportResponses,&parseReportToStorageLine);
    log("backtestStrategy::Saved Backtests to File");

    MPI_Barrier(MPI_COMM_WORLD);

    //(reportCount*datapointcount)
    log("backtestStrategy::Computing proffitability");
    double sumChange=0;
    for(report r:reportResponses){
        for(int i=r.target.clusterStart;i<r.target.clusterEnd+1-dayDifference;i++){
            //log("time base <"+std::to_string(clusteredDatapoints.at(i).time)+"> test <"+std::to_string(clusteredDatapoints.at(i).time));
            if(clusteredDatapoints.at(i).time==timeTestEnd){
                sumChange += clusteredDatapoints.at(i+dayDifference).change/(double)reportResponses.size();
                //log(std::to_string(clusteredDatapoints.at(i+dayDifference).change));
                //log("backtestStrategy::Report <"+std::to_string(r.base.id)+"> on <"+std::to_string(r.target.id)+"> sumchange added Currently at <"+std::to_string(sumChange));
            }
        }
    }
    log("backtestStrategy::Computed proffitability");
    MPI_Barrier(MPI_COMM_WORLD);

    return sumChange;


}**/

void initialization(std::string inputFileName){

    std::vector<datapoint>().swap(clusteredDatapoints);
    std::vector<ticker>().swap(finalTickerSchema);

    //O(1)
    log("initialization::Declaring Datatypes");
    declareDatatypes();
    log("initialization::Declared Datatypes");

//1535068800
    //Unknown BC IO
    log("initialization::Loading File to Datapoints Vector");
    std::vector<datapoint> unClusteredDatapoints;
    if(rank ==0) loadFileToDataStructure(&inputFileName,&unClusteredDatapoints,&parseSourceCSVLineToDataPoint);
    log("initialization::Loaded File to Datapoints Vector");


    //Unknown BC MPI
    log("initialization::Sharing UnClustered Datapoints");
    int datapointCount = unClusteredDatapoints.size();
    MPI_Bcast(&datapointCount,1,MPI_INT,0,MPI_COMM_WORLD);
    if(rank!=0) unClusteredDatapoints.resize(datapointCount);
    MPI_Bcast(&unClusteredDatapoints[0],datapointCount,datapointDatatype,0,MPI_COMM_WORLD);
    log("initialization::Shared UnClustered Datapoints");

    //~O(datapointcount/cores)
    log("initialization::Getting Local UnClustered Datapoints");
    std::vector<datapoint> localUnClusteredDatapoints;
    for(int i = rank;i<datapointCount;i+=size){
        localUnClusteredDatapoints.push_back(unClusteredDatapoints.at(i));
    }
    log("initialization::Got Local UnClustered Datapoints");


    //~O(datapointcount*tickercount/cores)
    log("initialization::Identifying Local UnClustered Tickers");
    std::vector<ticker> localUnClusteredTickers;
    identifyTickersFromDatapoints(&localUnClusteredDatapoints,&localUnClusteredTickers);
    log("initialization::Identified Local UnClustered Tickers");

    //Unknown BC MPI
    log("initialization::Regathering UnClustered Tickers");
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
    log(std::to_string(tickerCountsSum));
    tickers.resize(tickerCountsSum);
    MPI_Allgatherv(&localUnClusteredTickers[0],localUnClusteredTickersCount,tickerDatatype,&tickers[0],&unClusteredTickersCounts[0],&unClusteredTickersDispls[0],tickerDatatype,MPI_COMM_WORLD);
    log("initialization::Regathered UnClustered Tickers");

    //Unknown BC std::
    log("initialization::DeDuplicating UnClustered Tickers");
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
    log("initialization::DeDuplicated UnClustered Tickers");


    //~O(tickercount/cores)
    log("initialization::Identifying Local Tickers");
    std::vector<ticker> localTickers;
    int commonPartition = (tickers.size()/size>0)?tickers.size()/size:1;
    for(int i =0;i<tickers.size();i++){
        if(rank==i%size) localTickers.push_back(tickers.at(i));
    }
    log("initialization::Identified Local Tickers");


    //The next two sections could be combined
    //IE identify and sort on one run, 
    //thing is I dont want to implemnt my own sorting alogoritm

    //~O(datapointcount*tickercount/cores)
    log("initialization::Identifying Local Datapoints");
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
    log("initialization::Identified Local Datapoints");


    //~O(?) sorting
    log("initialization::Sorting Datapoints");
    std::sort(
        localDatapoints.begin(),
        localDatapoints.end(),
        [](datapoint &a,datapoint &b){
            if(a.id<b.id) return true;
            if(a.id>b.id) return false;
            return a.time<b.time;
        }
    );
    log("initialization::Sorted Datapoints");


    //regather O(unkown)
    log("initialization::Regathering Datapoints");
    int localDatapointCount = localDatapoints.size();
   
    if(rank==0) clusteredDatapoints.resize(datapointCount);
    std::vector<int> datapointCounts;
    datapointCounts.resize(size);
    std::vector<int> datapointDispls;
    if(rank==0) datapointDispls.push_back(0);
    MPI_Gather(&localDatapointCount,1,MPI_INT,&datapointCounts[0],1,MPI_INT,0,MPI_COMM_WORLD);
    for(int i=1;i<size;i++){
        if(rank==0) datapointDispls.push_back(datapointDispls.at(i-1)+datapointCounts.at(i-1));
    }
    MPI_Gatherv(&localDatapoints[0],localDatapointCount,datapointDatatype,&clusteredDatapoints[0],&datapointCounts[0],&datapointDispls[0],datapointDatatype,0,MPI_COMM_WORLD);
    std::vector<datapoint>().swap(localDatapoints);
    std::vector<datapoint>().swap(unClusteredDatapoints);
    log("initialization::Regathered Datapoints");



    log("initialization::Generating Ticker Schema");
    if(rank==0) generateTickerSchema(&clusteredDatapoints,&finalTickerSchema);
    log("initialization::Generated Ticker Schema");
    


}

void startup(std::string datapointFileName,std::string tickerSchemaFileName){

    std::vector<datapoint>().swap(clusteredDatapoints);
    std::vector<ticker>().swap(finalTickerSchema);

    log("startup::Declaring Datatypes");
    declareDatatypes();
    log("startup::Declared Datatypes");

    log("startup::Loading File to Datapoints Vector");
    if(rank==0) loadFileToDataStructure(&datapointFileName,&clusteredDatapoints,&parseStorageLineToDataPoint);
    log("startup::Loaded File to Datapoints Vector");

    log("startup::Loading File ticker schemas Vector");
    if(rank==0) loadFileToDataStructure(&tickerSchemaFileName,&finalTickerSchema,&parseStorageLineToTicker);
    log("startup::Loaded File to ticker schemas Vector");

    log("startup::sharing vectors");
    shareDatapoints();
    log("startup::shared vectors");
}

void pruneGlobalVectorsToGivenLength(std::string datapointFileName,std::string tickerFileName,int lines){

    log("pruneGlobalVectorsToGivenLength::Resizing Datapoints Vector");
    clusteredDatapoints.resize(lines);
    log("pruneGlobalVectorsToGivenLength::Resized Datapoints Vector");

    log("pruneGlobalVectorsToGivenLength::Recreating Ticker Schema");
    std::vector<ticker> newTickers;
    for(int i=0;i<finalTickerSchema.size();i++){
        if(finalTickerSchema.at(i).clusterEnd>=lines){
            finalTickerSchema.at(i).clusterEnd=lines-1;
            double meanSum=0;
            for(int j = finalTickerSchema.at(i).clusterStart;j<finalTickerSchema.at(i).clusterEnd;j++) meanSum += clusteredDatapoints.at(j).change;
            finalTickerSchema.at(i).mean = meanSum/finalTickerSchema.at(i).clusterEnd-finalTickerSchema.at(i).clusterStart;
            double stdSum=0;
            for(int j = finalTickerSchema.at(i).clusterStart;j<finalTickerSchema.at(i).clusterEnd;j++){
                stdSum += (clusteredDatapoints.at(j).change-finalTickerSchema.at(i).mean)*(clusteredDatapoints.at(j).change-finalTickerSchema.at(i).mean);
            
            }
            finalTickerSchema.at(i).std= sqrt(stdSum/(double)(lines-finalTickerSchema.at(i).clusterStart));

            break;
        }else{
            newTickers.push_back(finalTickerSchema.at(i));
        }
    }
    finalTickerSchema.swap(newTickers);
    log("pruneGlobalVectorsToGivenLength::Recreated Ticker Schema");

}

void filterGlobalVectorsByGivenTickers(std::vector<std::string> filterTickers){

    log("filterGlobalVectorsByGivenTickers::Populating New Datapoints and Ticker Vectors");
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
            for(ticker currentTicker:finalTickerSchema){
                if(currentTicker.id==myID){
                    ticker newTicker = currentTicker;
                    newTicker.clusterStart=newDatapoints.size();
                    for(int i = currentTicker.clusterStart;i<currentTicker.clusterEnd+1;i++){
                        newDatapoints.push_back(clusteredDatapoints.at(i));
                    }
                    newTicker.clusterEnd=newDatapoints.size()-1;
                    newTickers.push_back(newTicker);
                    break;
                }
            }
        }
    }
    log("filterGlobalVectorsByGivenTickers::Populated New Datapoints and Ticker Vectors");

    log("filterGlobalVectorsByGivenTickers::Setting Global Vectors to New Vectors");
    clusteredDatapoints.swap(newDatapoints);
    finalTickerSchema.swap(newTickers);
    log("filterGlobalVectorsByGivenTickers::Set Global Vectors to New Vectors");

}

void writeGlobalVectorsToFiles(std::string datapointFileName,std::string tickerFileName){
    log("writeGlobalVectorsToFiles::Loading Datapoints Vector to File");
    loadDatastructsToFile(&datapointFileName,&clusteredDatapoints,&parseDataPointToStorageLine);
    log("writeGlobalVectorsToFiles::Loaded Datapoints Vector to File");

    log("writeGlobalVectorsToFiles::Loading ticker schemas Vector to File");
    loadDatastructsToFile(&tickerFileName,&finalTickerSchema,&parseTickerToStorageLine);
    log("writeGlobalVectorsToFiles::Loaded ticker schemas Vector to File");

}










int main(int argc,char** argv){

    MPI_Init(&argc,&argv);

    MPI_Comm_rank(MPI_COMM_WORLD,&rank);
    MPI_Comm_size(MPI_COMM_WORLD,&size);
    //initialization("../assets/TestData","../assets/datapointdeepstorage","../assets/tickerschemadeepstorage");
    initialization("../assets/historical_daily_data_kaggle");
    writeGlobalVectorsToFiles("../assets/datapointdeepstorage","../assets/tickerschemadeepstorage");
   // startup("../assets/datapointdeepstorage","../assets/tickerschemadeepstorage");
    
    
    //MPI_Barrier(MPI_COMM_WORLD);
    //char tick[] = "MOGLC";
    //deleteDatapoint("../assets/datapointdeepstorage","../assets/tickerschemadeepstorage",tick);
    //createSampleFile("../assets/datapointdeepstorage","../assets/sampledpfile","../assets/sampletickfile",1000000);
    //startup("../assets/sampledpfile","../assets/sampletickfile");
   // std::vector<ticker> toTest;
   // toTest.push_back(finalTickerSchema.at(5));
    
   // toTest.push_back(finalTickerSchema.at(8));
   // double i  = backtestStrategyOnTickers(&toTest,clusteredDatapoints.at(toTest.at(0).clusterStart+5).time,0,clusteredDatapoints.at(toTest.at(0).clusterEnd-2).time,0.0,0,1,1,.05,.51);
    //double i = backtestStrategy(0,1534377600,0,0,1,1,.05,.51,"../assets/reports");
  
   // log(std::to_string(i));
    datapoint test;
   // test.change=i;
    test.close=0;
    test.open=0;
    test.id=0;
    strcpy(test.name,"RESULTS");
    test.time=0;
    std::vector<datapoint> towrite;
    towrite.push_back(test);
    std::string filename = "../assets/output";
   // loadDatastructsToFile(&filename,&towrite,&parseDataPointToStorageLine);
    MPI_Barrier(MPI_COMM_WORLD);
   /** log("creating test point");
    datapoint test;
    strcpy(test.name,"TSLA");
    test.change = .0389;
    for(int i =0;i<TICKER_NAME_LENGTH;i++) test.id |= (test.name[i] << (i*8));
    MPI_Barrier(MPI_COMM_WORLD);
    log("about to backtest");
    //backtestDatapoint(test,.05,.0001,1);**/

    MPI_Finalize();

}
////////////////////////////////////////////////////////////////////////////////////////////////////