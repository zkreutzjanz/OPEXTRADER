#include <mpi.h>
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


//no
//Definitions
const int TICKER_NAME_LENGTH =8;
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

//Globals
std::vector<ticker> finalTickerSchema;
std::vector<datapoint> clusteredDatapoints;
int rank;
int size;


#define LOG 1
#define TIME 1
/**
 * @brief For implicit compile time logging
 * 
 * @param message 
 */
void log(std::string message){
    std::string timestamp="";
    #if TIME
    struct timeval tp;
    gettimeofday(&tp,NULL);
    timestamp = " TIME: "+std::to_string(tp.tv_usec);;
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
            //log("DP: "+(std::string)out->name+"ID: "+std::to_string(out->id)+"TM: "+std::to_string(out->time));
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
 * @brief Loads file of name [fileName] to a vector of data type T[output]
 * 
 * @param fileName string name of file to parse
 * @param output pointer to datapoints vector to output data to
 */
template<typename T>
void loadFileToDataStructure(std::string *fileName,std::vector<T> *output,bool lineParseFunction(T *,std::string,std::string)){
    std::ifstream myFile;
	log("loadFileToDataStructure::Opening file <"+*fileName+">");
	myFile.open(*fileName);
	log("loadFileToDataStructure::File <"+*fileName+"> opened");
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
        //log(line);
        
		#if LOG
        if(!readable) log("loadFileToDataStructure::PARSE FAILURE on <"+line+">");
		if(readable) linesParsed++;
		if(linesParsed%10==0) log("loadFileToDataStructure::Parsed <"+std::to_string(linesParsed)+"> Lines");
		#endif
    }
    log("loadFileToDataStructure:: <"+std::to_string(linesParsed)+"> Lines Parsed");
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
    log(std::to_string(input->size()));
	log("loadDatastructsToFile::Opening file <"+*fileName+">");
    MPI_File myFile;
    char fntmp[fileName->length()];
    strcpy(fntmp,fileName->c_str());
    MPI_File_open(MPI_COMM_WORLD,fntmp,MPI_MODE_RDWR|MPI_MODE_CREATE,MPI_INFO_NULL,&myFile);
	log("loadDatastructsToFile::File <"+*fileName+"> opened");
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
        //log(temp);
        //log(std::to_string(input->at(i).id)+"ggg");
        #if LOG
		linesWritten++;
		if(linesWritten%10==0) log("loadDatastructsToFile::wrote <"+std::to_string(linesWritten)+"> Lines");
		#endif
    }}
    
    MPI_File_close(&myFile);
}



/**
 * @brief Clusters unclustered [datapointInput] vector into clustered [datapointOutput] vector with the vector [tickerSchemaOutput] defining cluster areas
 * 
 * @param datapointInput pointer to datapoints vector to take as input
 * @param datapointOutput pointer to datapoints vector to output data to
 * @param tickerSchemaOutput pointer to ticker vector to output schema data to
 */
void clusterDatapoints(std::vector<datapoint> *datapoints,std::vector<ticker> *tickerSchemaOutput){
    std::sort(
        datapoints->begin(),
        datapoints->end(),
        [](datapoint &a,datapoint &b){
            if(a.id<b.id) return true;
            if(a.id>b.id) return false;
            return a.time<b.time;
        }
    );
    int prev =0;
    ticker current;
    current.id=0;
    for(datapoint d : *datapoints){
        if(d.id==prev){

        }else{
            if(current.id!=0) tickerSchemaOutput->push_back(current);
            
        }
    }
    
   /** bool processed [datapointInput->size()] = {false};
    for(int i=0;i<datapointInput->size();i++){
        if(!processed[i]){
            ticker newTicker;
            newTicker.id=datapointInput->at(i).id;
            newTicker.clusterStart=datapointOutput->size();
            int datapointsAdded=0;
            for(int j =i;j<datapointInput->size();j++){
                if(datapointInput->at(i).id==datapointInput->at(j).id){
                    datapointOutput->push_back(datapointInput->at(j));
                    datapointsAdded++;
                    processed[j]=true;
                }
            }
            newTicker.clusterEnd = newTicker.clusterStart+datapointsAdded-1;
            tickerSchemaOutput->push_back(newTicker);
        }
        
    }**/
    
}

/**
 * @brief Declare datatypes used for MPI calls
 * 
 */
void declareDatatypes(){

	int lengths[] = {1,TICKER_NAME_LENGTH,1,1,1,1};
	MPI_Aint types[] = {offsetof(datapoint,id),offsetof(datapoint,name),offsetof(datapoint,time),offsetof(datapoint,open),offsetof(datapoint,close),offsetof(datapoint,change)};
	int disps[] = {MPI_LONG_LONG,MPI_CHAR,MPI_LONG_LONG,MPI_DOUBLE,MPI_DOUBLE,MPI_DOUBLE};
	MPI_Type_create_struct(6,lengths,types,disps,&datapointDatatype);
	MPI_Type_commit(&datapointDatatype);
    int tlengths[] = {1,1,1,1,1};
	MPI_Aint ttypes[] = {offsetof(ticker,id),offsetof(ticker,clusterStart),offsetof(ticker,clusterEnd),offsetof(ticker,mean),offsetof(ticker,std)};
	int tdisps[] = {MPI_LONG_LONG,MPI_INT,MPI_INT,MPI_DOUBLE,MPI_DOUBLE};
	MPI_Type_create_struct(5,tlengths,ttypes,tdisps,&tickerDatatype);
	MPI_Type_commit(&tickerDatatype);
}



void shareDatapoints(){
    int finalTickerCount = finalTickerSchema.size();
    MPI_Bcast(&finalTickerCount,1,MPI_INT,0,MPI_COMM_WORLD);
    int finalDatapointCount = clusteredDatapoints.size();
    MPI_Bcast(&finalDatapointCount,1,MPI_INT,0,MPI_COMM_WORLD);
    MPI_Barrier(MPI_COMM_WORLD);
    if(rank>0) finalTickerSchema.resize(finalTickerCount);
    MPI_Bcast(finalTickerSchema.data(),finalTickerCount,tickerDatatype,0,MPI_COMM_WORLD);
    MPI_Barrier(MPI_COMM_WORLD);
    if(rank>0) clusteredDatapoints.resize(finalDatapointCount);
    MPI_Bcast(&(clusteredDatapoints[0]),finalDatapointCount,datapointDatatype,0,MPI_COMM_WORLD);
}



/**
 * @brief Runs on program start. 1. Parses input file to datapoints 2. Clusters datapoints 3. Sends copy to all nodes
 * 
 * @param fileName Name of file to parse
 *
void initialization(std::string inputFileName,std::string datapointsFileName, std::string tickerSchemasFileName){

    
    log("initialization::Declaring Datatypes");
    declareDatatypes();
    log("initialization::Declared Datatypes");

    log("initialization::Loading File to Datapoints Vector");
    std::vector<datapoint> unClusteredDatapoints;
    if(rank==0){
        loadFileToDataStructure(&inputFileName,&unClusteredDatapoints,&parseSourceCSVLineToDataPoint);
    }
    log("initialization::Loaded File to Datapoints Vector");

    log("initialization::scattering datapoints");
    int datapointCount = unClusteredDatapoints.size();
    MPI_Bcast(&datapointCount,1,MPI_INT,0,MPI_COMM_WORLD);
    int datapointCounts[size];
    int datapointDispls[size];
    int commonPartition = datapointCount/size;
    datapointCounts[0]=commonPartition;
    datapointDispls[0]=0;
    for(int i =1;i<size;i++){
        datapointCounts[i]=i==size-1?commonPartition+datapointCount%commonPartition:commonPartition;
        datapointDispls[i]=datapointDispls[i-1]+datapointCounts[i-1];
    }
    std::vector<datapoint> unClusteredDatapointsPartition(datapointCounts[rank]);
    MPI_Scatterv(&unClusteredDatapoints[0],datapointCounts,datapointDispls,datapointDatatype,&unClusteredDatapointsPartition[0],datapointCounts[rank],datapointDatatype,0,MPI_COMM_WORLD);
    log("initialization::scattered datapoints");
    
    log("initialization::clustering per node");
    std::vector<datapoint> clusteredDatapointsPartition;
    std::vector<ticker> clusteredDatapointsTickerSchema;
    clusterDatapoints(&unClusteredDatapointsPartition,&clusteredDatapointsPartition,&clusteredDatapointsTickerSchema);
    unClusteredDatapointsPartition.clear();
    ///https://stackoverflow.com/questions/10464992/c-delete-vector-objects-free-memory#:~:text=vector%3CtempObject%3E().swap(tempVector)%3B
    std::vector<datapoint>().swap(unClusteredDatapointsPartition);
    log("initialization::clustered per node");

    log("initialization::gathering count of tickers per node");
    int tickerCount = clusteredDatapointsTickerSchema.size();
    int tickerCounts[size];
    MPI_Gather(&tickerCount,1,MPI_INT,&tickerCounts[0],1,MPI_INT,0,MPI_COMM_WORLD);
    int nonFinalTickerCount = 0;
    if(rank==0) for(int i=0;i<size;i++) nonFinalTickerCount += tickerCounts[i]; 
    log("initialization::gathered count of tickers per node");


    log("initialization::gathering ticker schemas");
    std::vector<ticker> nonFinalTickerSchemas(nonFinalTickerCount);
    int tickerDispls[size];
    tickerDispls[0]=0;
    for(int i=1;i<size;i++) tickerDispls[i] = tickerDispls[i-1]+tickerCounts[i-1];
    MPI_Gatherv(&clusteredDatapointsTickerSchema[0],tickerCount,tickerDatatype,&nonFinalTickerSchemas[0],tickerCounts,tickerDispls,tickerDatatype,0,MPI_COMM_WORLD);
    log("initialization::gathered ticker schemas");

    log("initialization::gathering datapoints");
    MPI_Gatherv(&clusteredDatapointsPartition[0],datapointCounts[rank],datapointDatatype,&unClusteredDatapoints[0],datapointCounts,datapointDispls,datapointDatatype,0,MPI_COMM_WORLD);
    log("initialization::gathered datapoints");


    log("initialization::reconfiguring ticker schemas");
    MPI_Barrier(MPI_COMM_WORLD);
    if(rank==0){
        int offset=0;
        for(int i=0;i<nonFinalTickerCount;i++){
            if(nonFinalTickerSchemas.at(i).clusterStart==0&&i){
                offset = nonFinalTickerSchemas.at(i-1).clusterEnd+1;
            }
            nonFinalTickerSchemas.at(i).clusterEnd += offset;
            nonFinalTickerSchemas.at(i).clusterStart += offset;
        }
    }
    log("initialization::reconfigured ticker schemas");
    
    log("initialization::clustering final datapoints");
    if(rank==0){
        bool processed [nonFinalTickerCount] = {false};
        for(int i=0;i<nonFinalTickerCount;i++){
            if(!processed[i]){
                ticker newTicker = nonFinalTickerSchemas.at(i);
                newTicker.clusterStart = clusteredDatapoints.size();
                int datapointsAdded=0;
                double changeSum= 0;
                for(int j=i;j<nonFinalTickerCount;j++){
                    if(nonFinalTickerSchemas.at(i).id==nonFinalTickerSchemas.at(j).id){
                        for(int k=nonFinalTickerSchemas.at(j).clusterStart;k<nonFinalTickerSchemas.at(j).clusterEnd+1;k++){
                            clusteredDatapoints.push_back(unClusteredDatapoints.at(k));
                            changeSum += unClusteredDatapoints.at(k).change;
                            datapointsAdded++;
                        }
                        processed[j]=true;
                    }
                }
                newTicker.clusterEnd = newTicker.clusterStart +datapointsAdded-1;
                newTicker.mean = changeSum/(double)datapointsAdded;
                double stdMidstep=0;
                for(int j =newTicker.clusterStart;j<newTicker.clusterEnd+1;j++){
                    stdMidstep += (clusteredDatapoints.at(j).change-newTicker.mean)*(clusteredDatapoints.at(j).change-newTicker.mean);
                }
                newTicker.std = sqrt(stdMidstep/((double)datapointsAdded));
                finalTickerSchema.push_back(newTicker);
            }
        }
    }
    log("initialization::clustered final datapoints");

    log("initialization::Loading Datapoints Vector to File");
    loadDatastructsToFile(&datapointsFileName,&clusteredDatapoints,&parseDataPointToStorageLine);
    log("initialization::Loaded Datapoints Vector to File");

    log("initialization::Loading ticker schemas Vector to File");
    loadDatastructsToFile(&tickerSchemasFileName,&finalTickerSchema,&parseTickerToStorageLine);
    log("initialization::Loaded ticker schemas Vector to File");



}\
*/

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
            //log("identifyTickersFromDatapoints::New Ticker <"+std::to_string(newticker.id)+">");
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
    int sum=0;
    for(int i=1;i<in->size();i++){
        if(in->at(i).id!=currentTicker.id){
            currentTicker.clusterEnd=i-1;
            currentTicker.mean=(double)sum/(double)(currentTicker.clusterEnd-currentTicker.clusterStart+1);
            double stdSum;
            for(int j = currentTicker.clusterStart;j<currentTicker.clusterEnd+1;j++){
                stdSum += pow(in->at(j).change-currentTicker.mean,2.0);
            }
            currentTicker.std= sqrt(stdSum/(double)(currentTicker.clusterEnd-currentTicker.clusterStart+1));
            out->push_back(currentTicker);
            currentTicker.id=in->at(i).id;
            currentTicker.clusterStart=i;
            currentTicker.clusterEnd=0;
            currentTicker.mean=0;
            currentTicker.std=0;
            sum=0;
        }
        sum += in->at(i).change;
    }
}


void initializationClusterAndSort(std::string inputFileName,std::string datapointsFileName, std::string tickerSchemasFileName){

    //O(1)
    log("initialization::Declaring Datatypes");
    declareDatatypes();
    log("initialization::Declared Datatypes");

    //O unknown
    log("initialization::Loading File to Datapoints Vector");
    std::vector<datapoint> unClusteredDatapoints;
    if(rank ==0) loadFileToDataStructure(&inputFileName,&unClusteredDatapoints,&parseSourceCSVLineToDataPoint);
    
    log("initialization::Loaded File to Datapoints Vector");
    MPI_Barrier(MPI_COMM_WORLD);
    //O unkown
    log("initialization::Sharing Datapoints");
    MPI_Barrier(MPI_COMM_WORLD);
    int datapointCount = unClusteredDatapoints.size();
    MPI_Bcast(&datapointCount,1,MPI_INT,0,MPI_COMM_WORLD);
    if(rank!=0) unClusteredDatapoints.resize(datapointCount);
    MPI_Bcast(&unClusteredDatapoints[0],datapointCount,datapointDatatype,0,MPI_COMM_WORLD);
    log("initialization::Shared Datapoints");
    MPI_Barrier(MPI_COMM_WORLD);

    //~O(datapointcount*tickercount)
    log("initialization::Identifying Tickers");
    MPI_Barrier(MPI_COMM_WORLD);
    std::vector<ticker> tickers;
    identifyTickersFromDatapoints(&unClusteredDatapoints,&tickers);
    log(std::to_string(tickers.size()));
    log("initialization::Identified Tickers");
    MPI_Barrier(MPI_COMM_WORLD);

    //~O(tickercount/cores)
    log("initialization::Identifying Local Tickers");
    MPI_Barrier(MPI_COMM_WORLD);
    std::vector<ticker> localTickers;
    int commonPartition = (tickers.size()/size>0)?tickers.size()/size:1;
    for(int i =0;i<tickers.size();i++){
        if(rank==i%size) localTickers.push_back(tickers.at(i));
    }

    log("initialization::Identified Local Tickers");
    log(std::to_string(localTickers.size()));
    MPI_Barrier(MPI_COMM_WORLD);


    //The next two sections could be combined
    //IE identify and sort on one run, 
    //thing is I dont want to implemnt my own sorting alogoritm

    //~O(datapointcount*tickercount/cores)
    log("initialization::Identifying Local Datapoints");
    MPI_Barrier(MPI_COMM_WORLD);
    std::vector<datapoint> localDatapoints;
    for(int i=0;i<unClusteredDatapoints.size();i++){
        for(int j = 0;j<localTickers.size();j++){
            if(localTickers.at(j).id==unClusteredDatapoints.at(i).id){
                localDatapoints.push_back(unClusteredDatapoints.at(i));
                j=localTickers.size();
            }
        }
    }
    //std::vector<ticker>().swap(localTickers);
    //std::vector<ticker>().swap(tickers);
    log("initialization::Identified Local Datapoints");
    
    MPI_Barrier(MPI_COMM_WORLD);


    //~O(?) sorting
    log("initialization::Sorting Datapoints");
    MPI_Barrier(MPI_COMM_WORLD);
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
    log(std::to_string(localDatapoints.size()));
    MPI_Barrier(MPI_COMM_WORLD);


    //regather O(unkown)
    log("initialization::Regathering Datapoints");
    MPI_Barrier(MPI_COMM_WORLD);
    int localDatapointCount = localDatapoints.size();
    std::vector<datapoint> finalDatapoints;
    std::vector<int> datapointCounts;
    datapointCounts.resize(size);
	int* datapointDispls = new int[size];
    if(rank==0) datapointDispls[0]=0;
    log(std::to_string(localDatapointCount));
    MPI_Barrier(MPI_COMM_WORLD);
    MPI_Gather(&localDatapointCount,1,MPI_INT,&datapointCounts[0],size,MPI_INT,0,MPI_COMM_WORLD);
    for(int i=1;i<size;i++){
        if(rank==0) datapointDispls[i]=datapointDispls[i-1]+datapointCounts.at(i-1);
        if(rank==0) log(std::to_string(i)+"::"+std::to_string(datapointCounts.at(i))+"::"+std::to_string(datapointDispls[i]));
    }
    if(rank==0) finalDatapoints.resize(datapointCount);
    MPI_Gatherv(&localDatapoints[0],localDatapointCount,datapointDatatype,&finalDatapoints[0],&datapointCounts[0],datapointDispls,datapointDatatype,0,MPI_COMM_WORLD);
    std::vector<datapoint>().swap(localDatapoints);
    std::vector<datapoint>().swap(unClusteredDatapoints);
    log("initialization::Regathered Datapoints");
MPI_Barrier(MPI_COMM_WORLD);


    log("initialization::Generating Ticker Schema");
    MPI_Barrier(MPI_COMM_WORLD);
    std::vector<ticker> finalTickers;
    if(rank==0) generateTickerSchema(&finalDatapoints,&finalTickers);
    log("initialization::Generated Ticker Schema");
MPI_Barrier(MPI_COMM_WORLD);
    log("initialization::Loading Datapoints Vector to File");
    MPI_Barrier(MPI_COMM_WORLD);
    log(std::to_string(finalDatapoints.size()));
    loadDatastructsToFile(&datapointsFileName,&finalDatapoints,&parseDataPointToStorageLine);
    log("initialization::Loaded Datapoints Vector to File");
MPI_Barrier(MPI_COMM_WORLD);
    log("initialization::Loading ticker schemas Vector to File");
    MPI_Barrier(MPI_COMM_WORLD);
    loadDatastructsToFile(&tickerSchemasFileName,&finalTickers,&parseTickerToStorageLine);
    log("initialization::Loaded ticker schemas Vector to File");
    MPI_Barrier(MPI_COMM_WORLD);
}


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

double getZScore (double p)
{
    
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
    return (1-erf(-(z)/ sqrt(2.0)))/2.0;
}


void startup(std::string datapointFileName,std::string tickerSchemaFileName){
    log("startup::Declaring Datatypes");
    declareDatatypes();
    log("startup::Declared Datatypes");

    log("startup::Loading File to Datapoints Vector");
    if(rank==0){
        loadFileToDataStructure(&datapointFileName,&clusteredDatapoints,&parseStorageLineToDataPoint);
    }
    log("startup::Loaded File to Datapoints Vector");

    log("startup::Loading File ticker schemas Vector");
    if(rank==0){
        loadFileToDataStructure(&tickerSchemaFileName,&finalTickerSchema,&parseStorageLineToTicker);
    }
    log("startup::Loaded File to ticker schemas Vector");

    log("startup::sharing vectors");
    shareDatapoints();
    log("startup::shared vectors");

}

void runNextDaySuggestions(ticker in,long long timebar,std::vector<ticker> *out,double inLowerBound,double inUpperBound,double targetMin,double targetMax){
   
    std::vector<datapoint> datapointsInBound;
    for(int i=in.clusterStart;i<in.clusterEnd+1;i++){
        if(clusteredDatapoints.at(i).change<=inUpperBound&&clusteredDatapoints.at(i).change>=inLowerBound&&clusteredDatapoints.at(i).time<timebar){
            datapointsInBound.push_back(clusteredDatapoints.at(i));
        }
    }
    int totalInDatapoints = in.clusterEnd-in.clusterStart +1;
    int totalInDatapointsInBound = datapointsInBound.size();
    int partition = finalTickerSchema.size()/size;
    for(int t=rank*partition;t<((rank==size-1)?finalTickerSchema.size():(rank+1)*partition);t++){
        if(t>=finalTickerSchema.size()) break;
        if(finalTickerSchema.at(t).id==in.id) continue;
        int targetDatapointsLookedAt=0;
        int targetDatapointsInBound=0;
        for(datapoint d:datapointsInBound){
            long long dayAfter  = (d.time-(9/24)*86400)%(7*86400)==0?(long long)d.time+86400*3:(long long)d.time+86400;
            for(int td=finalTickerSchema.at(t).clusterStart;td<finalTickerSchema.at(t).clusterEnd+1;td++){
               if(td>=clusteredDatapoints.size()) continue;
                if(clusteredDatapoints.at(td).time==dayAfter){
                    targetDatapointsLookedAt++;
                    if(clusteredDatapoints.at(td).change>=targetMin&&clusteredDatapoints.at(td).change<=targetMax){
                        targetDatapointsInBound++;
                    }
                }
            }
        }
        //log("here 2");
        double prob = .50;
        if(targetDatapointsLookedAt>0){
        double zout = ((targetDatapointsInBound/targetDatapointsLookedAt)-prob)/sqrt((1-prob)*prob/targetDatapointsLookedAt);
        if(zout>0){
            out->push_back(finalTickerSchema.at(t));
            log("Ticker In: "+(std::string)datapointsInBound.at(0).name
            +" Target: "+(std::string)clusteredDatapoints.at(finalTickerSchema.at(t).clusterStart).name
            +" Target Looked At: "+std::to_string(targetDatapointsLookedAt)
            +" Target In Bound: "+std::to_string(targetDatapointsInBound)
            +" source Looked At: "+std::to_string(totalInDatapoints)
            +" source In Bound: "+std::to_string(totalInDatapointsInBound)
            +" Z Score: "+std::to_string(zout)
           );
        } 
        }
    }   
    
}

void backtestDatapoint(datapoint in,double percentileMargin,double targetMin,double targetMax){
    std::vector<ticker> out;
    for(ticker t : finalTickerSchema){
        if(t.id==in.id){
            double percentile = getPercentile((in.change-t.mean)/t.std);
            double upperValue = getZScore(percentile + percentileMargin>1?1:percentile + percentileMargin)*t.std+t.mean;
            double lowerValue = getZScore(percentile - percentileMargin<0?0:percentile - percentileMargin)*t.std+t.mean;
            runNextDaySuggestions(t,in.time,&out,lowerValue,upperValue,targetMin,targetMax);
        }
    }
   
    long long dayAfter  = (in.time-(9/24)*86400)%(7*86400)==0?(long long)in.time+86400*3:(long long)in.time+86400;

}

void backtestGivenDay(){

}



int main(int argc,char** argv){

    MPI_Init(&argc,&argv);

    MPI_Comm_rank(MPI_COMM_WORLD,&rank);
    MPI_Comm_size(MPI_COMM_WORLD,&size);
    initializationClusterAndSort("../assets/TestData","../assets/datapointdeepstorage","../assets/tickerschemadeepstorage");
   
    //initializationClusterAndSort("../assets/historical_daily_data_kaggle","../assets/datapointdeepstorage","../assets/tickerschemadeepstorage");
    //startup("../assets/datapointdeepstorage","../assets/tickerschemadeepstorage");
    MPI_Barrier(MPI_COMM_WORLD);
    log("creating test point");
    datapoint test;
    strcpy(test.name,"TSLA");
    test.change = .0389;
    for(int i =0;i<TICKER_NAME_LENGTH;i++) test.id |= (test.name[i] << (i*8));
    MPI_Barrier(MPI_COMM_WORLD);
    log("about to backtest");
    //backtestDatapoint(test,.05,.0001,1);
    MPI_Finalize();

}