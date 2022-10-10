
#ifndef oio
#define oio
#include <mpi.h>
#include <ctime>
#include <iomanip>
#include <vector>
#include "ostructs.hpp"

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
 * @brief Takes a *AnalysisParameter[in] and converts it to a Storage-type *string [line] with [delimiter]
 * @param *in AnalysisParameter in
 * @param delimiter the string delimiter
 * @param *out std::string output line pointer
 *
void parseAnalysisParameterToStorageLine(analysisParameter *in,std::string delimiter,std::string *out){
    out->append(std::to_string(in->base.id)+delimiter+std::to_string(in->baseMin)+delimiter+std::to_string(in->baseMax)+delimiter+std::to_string(in->baseInBound)+delimiter+std::to_string(in->baseTotal)+delimiter+std::to_string(in->baseDayDiff)+delimiter+std::to_string(in->targetMin)+delimiter+std::to_string(in->targetMax)+delimiter+std::to_string(in->targetInBound)+delimiter+std::to_string(in->targetTotal)+delimiter+std::to_string(in->targetDayDiff)+delimiter+std::to_string(in->targetDayOffset)+delimiter+std::to_string(in->targetPercentInc)+delimiter+std::to_string(in->targetZScore)+delimiter+std::to_string(in->timeMin)+delimiter+std::to_string(in->timeMax)+"\n");
}
*/

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

#endif