# ==================== Packages
# Text mining and data manipulation
library(dplyr)
library(plyr)
library(tm)
library(quanteda)
library(DescTools)
library(DataCombine)
library(stringr)
library(stringi)
# Visualization
library(visNetwork)
library(collapsibleTree)
# EDA
library(reshape2)
library(ggplot2)
# ==================== Set Directory
sqlCollection <- "1"
#sqlCollection <- "Neuer Ordner"
#sqlCollection <- "etl_210211_1623"
#sqlCollection <- "hbi"

pat <- "^.*Users.*?([A-Z]+).*"
username <- gsub(pat, "\\1", getwd())
filepath_root <- paste0("C:/Users/",username,"/Desktop/")
filepath_sql <- paste0(filepath_root,"ROMEO/", sqlCollection)
filepath_output <- paste(filepath_root, "ROMEO/output/", sep="")

files <- list.files(path=filepath_sql, pattern="*.txt|*.sql", full.names = TRUE, recursive=FALSE)
files_list <- data.frame(files)
# ==================== Configuration of patterns
unvalid_pattern <- read.csv(paste0(filepath_root,"ROMEO/romeo_unvalid_pattern.csv"), header = FALSE)
valid_pattern <- read.csv(paste0(filepath_root,"ROMEO/romeo_valid_pattern.csv"), header = FALSE)
filter_pattern <- read.csv(paste0(filepath_root,"ROMEO/romeo_filter_pattern.csv"), header = FALSE)
aggregation_pattern <- read.csv(paste0(filepath_root,"ROMEO/romeo_aggregation_pattern.csv"), header = FALSE)
create_pattern <- read.csv(paste0(filepath_root,"ROMEO/romeo_create_pattern.csv"), header = FALSE)
insert_pattern <- read.csv(paste0(filepath_root,"ROMEO/romeo_insert_pattern.csv"), header = FALSE)
schema_cleansing_pattern <- read.csv(paste0(filepath_root,"ROMEO/romeo_schema_cleansing_pattern.csv"), header = FALSE)
database_cleansing_pattern <- read.csv(paste0(filepath_root,"ROMEO/romeo_database_cleansing_pattern.csv"), header = FALSE)
table_cleansing_pattern <- read.csv(paste0(filepath_root,"ROMEO/romeo_table_cleansing_pattern.csv"), header = FALSE)
df.unvalid_pattern_sum <- data.frame(as.character())

# -------------------- Prepare final result, quality check and error handling tables for validation purposes
df <- data.frame(targetTableName=as.character(), sourceTableName=as.character(), filter=as.character())
df.quality_check <- data.frame(scriptName=as.character(), errorMessage=as.character(),countOfInsert=as.character())
df.quality_check_names <- names(df.quality_check)

# ==================== Extraction of Data
i <- 1
for (i in files) {
  ### for each sql file an error handling table
  df.error_handling <- data.frame(romeoStep=as.character(), rawOutput=as.character())
  df.error_handling_names <- names(df.error_handling)
  
  # -------------------- Text mining preparation
  createCorpus <- function(filepath_sql) {
    conn <- file(filepath_sql, "r")
    fulltext <- readLines(conn)
    close(conn)
    
    vs <- VectorSource(fulltext)
    Corpus(vs, readerControl=list(readPlain, language="en", load=TRUE))
  }
  
  #rawCorpus <- createCorpus(files[1]) # just for coding / debugging
  rawCorpus <- createCorpus(i)
  analysisCorpus <- corpus(rawCorpus)
  
  #################### START OF TEXT MINING INTELLIGENCE
  # -------------------- Text Mining: Extraction of the name of the script assuming it is the name of the target table
  sql_script_name <- corpus_segment(analysisCorpus, pattern = create_pattern) 
  if (length(sql_script_name)>0) { # start of dirty hack
    df.temp <- data.frame("1 Extract Pattern of Procedure", sql_script_name)
    names(df.temp) <- df.error_handling_names
    df.error_handling <- rbind(df.error_handling, df.temp)
    names(df.error_handling) <- df.error_handling_names
    sql_script_name <- gsub(" .*","",sql_script_name)
    sql_script_name <- sql_script_name[1]
    sql_script_name
    
    files_list[i,2] <- "processed"
    # -------------------- Text Mining: Extraction of TARGET TABLE NAME identified by "insert into"-prefix per line
    insert_into <- corpus_segment(analysisCorpus, pattern = insert_pattern) 
    insert_into <- data.frame(insert_into)
    ## ------------------- If there is no valid target table identifiable then noted as "no_target"
    if(nrow(insert_into) == 0){
      insert_into <- "no_target"
    } else { 
    }
    
    df.temp <- data.frame("2 Extract Pattern of Insert Into",insert_into)
    names(df.temp) <- df.error_handling_names
    df.error_handling <- rbind(df.error_handling, df.temp)
    insert_into <- data.frame(insert_into)
    
    ## ------------------- Target table identification rule 1: "#" and "@" do not represent valid table names
    insert_into <- insert_into[grepl("_", insert_into$insert_into),] 
    insert_into <- gsub("[;]","",insert_into) # Remove ;
    insert_into <- gsub("\\[|\\]", "", insert_into) # Remove brackets
    insert_into <- str_remove_all(insert_into, "[()]") # Remove round brackets
    insert_into <- data.frame(insert_into)
    insert_into$insert_into <- gsub(" .*", "\\1", insert_into[,1]) # Take first word
    insert_into$cleanPrep <- StrPos(insert_into[,1], "_", pos = 1)
    insert_into <- insert_into[!grepl("#|@", insert_into$insert_into),] 
    insert_into <- data.frame(insert_into)
    insert_into <- distinct(insert_into)
    insert_into <- insert_into[,1]
    count_temp <- length(insert_into)
    files_list[i,3] <- count_temp
    names(files_list) <- df.quality_check_names
    ## ------------------- If there is no valid target table identifiable then noted as "no_target"
    if(length(insert_into) == 1){
    } else { 
      insert_into <- "no_target"
    }
    targetTableName <- gsub(" .*","",insert_into)
    targetTableName
    
    # -------------------- Text Mining: Extraction of SOURCE TABLE NAME 
    y <- data.frame(x=as.character())
    for (i in valid_pattern){
      pattern.valid_pattern <- paste0(i, "*")
      x <- corpus_segment(analysisCorpus, pattern = pattern.valid_pattern) 
      y <- rbind(data.frame(x))
    }
    if(nrow(y) == 0){
      y <- "no_table"
    } else { 
    }
    df.temp <- data.frame("3 Extraction of Source Tables",y)
    names(df.temp) <- df.error_handling_names
    df.error_handling <- rbind(df.error_handling, df.temp)
    ## ------------------- Source table identification rule 1: "#" and "@" do not represent valid table names
    y <- y[grepl("_", y$x),] 
    y <- gsub("[;']","",y) # Remove ;
    y <- gsub("[,']","",y) # Remove ,
    y <- gsub("\\[|\\]", "", y) # Remove brackets
    y <- str_remove_all(y, "[()]") # Remove round brackets
    y <- data.frame(y)
    #y$sourceTableName <- gsub(" .*", "\\1", y[,1]) # Take first word
    y$sourceTableName <- sub("\\s.*","", y[,1])# Take first word
    y <- y[!grepl("#|@", y$y),] 
    y <- distinct(y, sourceTableName) #Filter for distinct sourceTables
    y <- y[grepl("_", y$sourceTableName),]
    y <- data.frame(y)
    names(y) <- "sourceTableName"
    ## ------------------- If there is no valid source table identifiable then noted as "no_table"
    if(nrow(y) == 0){
      y <- InsertRow(y, NewRow = "no_table")
      names(y) <- c("sourceTableName")
      y <- cbind(targetTableName = targetTableName, y)
    } else { 
      y <- cbind(targetTableName = targetTableName, y) 
    }
    
    # -------------------- Text Mining: Extraction of UNVALID PATTERN
    for (i in unvalid_pattern){
      pattern.unvalid_pattern <- paste0(i, "*")
      df.unvalid_pattern <- corpus_segment(analysisCorpus, pattern = pattern.unvalid_pattern) 
      df.unvalid_pattern <- data.frame(df.unvalid_pattern)
      ## ------------------- If there is no valid target table identifiable then noted as "no_target"
      if(nrow(df.unvalid_pattern) == 0){
        df.unvalid_pattern <- ""
      } else { 
      }
      df.temp <- data.frame("2 Extract Pattern of UNVALID PATTERN", df.unvalid_pattern)
      names(df.temp) <- df.error_handling_names
      df.error_handling <- rbind(df.error_handling, df.temp)
      ## ------------------- Target table identification rule 1: "#" and "@" do not represent valid table names
      #df.unvalid_pattern <- df.unvalid_pattern[grepl("_", df.unvalid_pattern$df.unvalid_pattern),] 
      df.unvalid_pattern <- gsub("[;]","",df.unvalid_pattern) # Remove ;
      df.unvalid_pattern <- str_remove_all(df.unvalid_pattern, "[()]") # Remove round brackets
      df.unvalid_pattern <- data.frame(df.unvalid_pattern)
      df.unvalid_pattern$df.unvalid_pattern <- gsub(" .*", "\\1", df.unvalid_pattern[,1]) # Take first word
      df.unvalid_pattern$cleanPrep <- StrPos(df.unvalid_pattern[,1], "_", pos = 1)
      df.unvalid_pattern <- df.unvalid_pattern[!grepl("#|@", df.unvalid_pattern$df.unvalid_pattern),] 
      df.unvalid_pattern <- data.frame(df.unvalid_pattern)
      df.unvalid_pattern <- distinct(df.unvalid_pattern)
      df.unvalid_pattern <- df.unvalid_pattern[,1]
      ## ------------------- If there is no valid table identifiable then noted as "no_target"
      if(length(df.unvalid_pattern) == 0){
        df.unvalid_pattern <- ""
      } else { 
      }
      df.unvalid_pattern <- data.frame(df.unvalid_pattern)
      names(df.unvalid_pattern) <- "sourceTableName"
    }
    df.unvalid_pattern_sum <- rbind(df.unvalid_pattern_sum, df.unvalid_pattern)
    
    # -------------------- Text Mining: Extract procedure complexity
    for (i in filter_pattern){
      filterPattern <- paste0(i, "*")
      df.filterPattern <- corpus_segment(analysisCorpus, pattern = filterPattern) 
      df.filterPattern <- data.frame(df.filterPattern)
      ## ------------------- If there is no valid target table identifiable then noted as "no_target"
      if(nrow(df.filterPattern) == 0){
        df.filterPattern <- "o"
      } else { 
        df.filterPattern <- "x"
      }
    }
    
    for (i in aggregation_pattern){
      aggregation_pattern <- paste0(i, "*")
      df.aggregation_pattern <- corpus_segment(analysisCorpus, pattern = aggregation_pattern) 
      df.aggregation_pattern <- data.frame(df.aggregation_pattern)
      ## ------------------- If there is no valid target table identifiable then noted as "no_target"
      if(nrow(df.aggregation_pattern) == 0){
        df.aggregation_pattern <- "o"
      } else { 
        df.aggregation_pattern <- "x"
      }
    }
    #################### END OF TEXT MINING INTELLIGENCE
    y$sql_script_name <- as.character(sql_script_name)
    y$filter <- as.character(df.filterPattern)
    y$aggregation <- as.character(df.aggregation_pattern)
    df <- rbind(df,y)
    # -------------------- Outputs: Make various output tables
    path <- paste(filepath_output,sql_script_name[1],".csv",sep="")
    write.csv(y, file = path, row.names = FALSE)
    
    path_error <- paste(filepath_output,sql_script_name[1],"_errorHandling",".csv",sep="")
    write.csv(df.error_handling, file = path_error, row.names = FALSE)
    
    path <- paste(filepath_output,"df",".csv",sep="")
    write.csv(df, file = path, row.names = FALSE)
    
    path <- paste(filepath_output,"df",".csv",sep="")
    write.csv(df, file = path, row.names = FALSE)
    
    path <- paste(filepath_output,"files_processed",".csv",sep="")
    write.csv(files_list, file = path, row.names = FALSE)
  }else{
    files_list[i,2] <- "something fishy here"
  } # end of dirty hack
  i <- i+1
} # the end

# ==================== Postprocessing of Data
## ------------------- Removing all unvalid table names
df <- df %>% anti_join(df.unvalid_pattern_sum, by="sourceTableName")

## -------------------- Decomposing Database.Schema.Tablename
df[, 'sourceTableName'] <- as.character(df[, 'sourceTableName'])
df$sourceDatabase <- ifelse(nchar(gsub("[^.]", "", df$sourceTableName))==2, str_extract(df$sourceTableName, "[^.]+"), "")

string <- unique(ifelse(df$sourceDatabase != "", paste0(".*", df$sourceDatabase,"\\."),""))
new_string <- as.character()
if (length(string) == 1){
  new_string <- string
}else{
  for(i in string){
    new_string <- paste0(new_string, i, "|")
  }
}

string <- unique(ifelse(df$targetDatabase != "", paste0(".*", df$targetDatabase,"\\."),""))
new_string2 <- as.character()
if (length(string) == 1){
  new_string2 <- string
}else{
  for(i in string){
    new_string2 <- paste0(new_string2, i, "|")
  }
}

df$sourceSchema <- ifelse(nchar(gsub("[^.]", "", df$sourceTableName))== 2, gsub(new_string, "", df$sourceTableName), df$sourceTableName)
df$sourceSchema <- ifelse(nchar(gsub("[^.]", "", df$sourceSchema))== 1, sub("\\..*", "", df$sourceSchema), "")
df$sourceTable <- sub(".*\\.", "", df$sourceTableName)
df$sourceLayer <- sub("_.*", "", df$sourceSchema)

df$targetSchema <- ifelse(nchar(gsub("[^.]", "", df$targetTableName))== 2, gsub(new_string, "", df$targetTableName), df$targetTableName)
df$targetSchema <- ifelse(nchar(gsub("[^.]", "", df$targetSchema))== 1, sub("\\..*", "", df$targetSchema), "")
df$targetTable <- sub(".*\\.", "", df$targetTableName)
df$targetLayer <- sub("_.*", "", df$targetTableName)

## -------------------- Manual cleansing
### -------------------- Schema cleansing
# if there is no sourceSchema then the entry is possibly just a supporting table
df <- df[!df$sourceSchema == "",] 
df <- df[!df$sourceSchema %in% schema_cleansing_pattern]
### -------------------- Database cleansing
#df <- df[df %in% database_cleansing_pattern]
### -------------------- Layer cleansing
df <- df[!df$sourceLayer %in% layer_cleansing_pattern]
### -------------------- Tablename cleansing
df <- df[!df$sourceLayer %in% table_cleansing_pattern]

## -------------------- Join structure preparation
df$clean_sourceTableName <- paste0(df$sourceSchema,".",df$sourceTable)
df <- df[!df$targetTableName == df$clean_sourceTableName,]

df.new_x <- df[,c(1,4,5,6,9,7,13)]
df.new_y <- df[,c(1,4,5,6,9,7,13)]

names(df.new_x) <- c(
  "targetTableName","filter","aggregation","sourceDatabase",
  "sourceLayer", "sourceSchema","key"
)

names(df.new_y) <- c(
  "key","filter","aggregation","sourceDatabase",
  "sourceLayer", "sourceSchema","sourceTableName"
)

df.Total <- join(df.new_x, df.new_y, by = "key", type = "left", match = "all")

df.Total <- rename(df.Total, c("key"="sourceTableName"))
names(df.Total)[length(names(df.Total))]<-"key" 
df.Total <- join(df.Total, df.new_y, by = "key", type = "left", match = "all")