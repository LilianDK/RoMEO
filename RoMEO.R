#####
#Purpose: Extract source table names with respect to target tables 
#####
library(dplyr)
library(tm)
library(quanteda)
library(DescTools)
library(DataCombine)
library(stringr)
### Load data
filepath <- "C:/Users/----/Desktop/"
filepath2 <- "C:/Users/-----/Desktop/SQLS/"
direction <- paste(filepath, "SQLS/output/", sep="")

files <- list.files(path=filepath2, pattern="*.txt", full.names = TRUE, recursive=FALSE)

df <- data.frame(targetTableName=as.character(), sourceTableName=as.character())
df_qualityCheck <- data.frame(procedure=as.character(), countOfInsert=as.character())
df_qualityCheckNames <- names(df_qualityCheck)

for (i in files) {
  df_errorHandling <- data.frame(romeoStep=as.character(), rawOutput=as.character())
  df_errorHandlingNames <- names(df_errorHandling)
  
  ### Preparation
  createCorpus <- function(filepath2) {
    conn <- file(filepath2, "r")
    fulltext <- readLines(conn)
    close(conn)
    
    vs <- VectorSource(fulltext)
    Corpus(vs, readerControl=list(readPlain, language="en", load=TRUE))
  }
  
  rawCorpus <- createCorpus(i)
  analysisCorpus <- corpus(rawCorpus)
  
  ### Extract Source Tables
  #library(quanteda)
  procedureName <- corpus_segment(analysisCorpus, pattern = "procedure*") #Segment only for patterns with "procedure"-prefix
  df_temp <- data.frame("1 Extract Pattern of Procedure",procedureName)
  names(df_temp) <- df_errorHandlingNames
  df_errorHandling <- rbind(df_errorHandling, df_temp)
  names(df_errorHandling) <- df_errorHandlingNames
  procedureName <- gsub(" .*","",procedureName)
  procedureName <- procedureName[1]
  
  insertInto <- corpus_segment(analysisCorpus, pattern = "insert into*") #Segment only for patterns with "insert into"-prefix
  insertInto <- data.frame(insertInto)
  
  if(nrow(insertInto) == 0){
    insertInto <- "no_target"
  } else { 
  }
  df_temp <- data.frame("2 Extract Pattern of Insert Into",insertInto)
  names(df_temp) <- df_errorHandlingNames
  df_errorHandling <- rbind(df_errorHandling, df_temp)
  insertInto <- insertInto[!grepl("#|@", insertInto$insertInto),] #Filter all expressions which do not contain #,@
  insertInto <- gsub("[;]","",insertInto) #Remove ;
  insertInto <- data.frame(insertInto)
  insertInto$insertInto <- gsub(" .*", "\\1", insertInto[,1]) #Take first word
  insertInto$cleanPrep <- StrPos(insertInto[,1], "_", pos = 1)
  insertInto <- insertInto[grepl("3", insertInto[,2]),] #Filter for position 3
  insertInto <- distinct(insertInto)
  insertInto <- insertInto[,1]
  count_temp <- length(insertInto)
  temp_qualityCheck <- data.frame(procedureName, count_temp)
  names(temp_qualityCheck) <- df_qualityCheckNames
  if(length(insertInto) == 1){
  } else { 
    insertInto <- "no_target"
  }
  targetTableName <- gsub(" .*","",insertInto)
  
  #if(length(procedureName) == 1){
  #} else { 
  #  targetTableName <- data.frame(procedureName)
  #  targetTableName <- as.character(targetTableName[1,1])
  #}
  
  # Extraction of all source tables
  x <- corpus_segment(analysisCorpus, pattern = "from*") #Segment only for patterns with "from"-prefix
  y <- data.frame(x)
  df_temp <- data.frame("3 Extraction of Source Tables",x)
  names(df_temp) <- df_errorHandlingNames
  df_errorHandling <- rbind(df_errorHandling, df_temp)
  y <- y[!grepl("#|@", y$x),] #Filter all expressions which do not contain #,@
  y <- gsub("[;']","",y) #Remove ;
  y <- gsub("\\[|\\]", "", y)
  y <- str_remove_all(y, "[()]")
  y <- data.frame(y)
  y$sourceTableName <- gsub(" .*", "\\1", y[,1]) #Take first word
  y$cleanPrep <- StrPos(y[,2], "_", pos = 1)
  y <- y[grepl("3", y[,3]),] #Filter for position 3
  y <- distinct(y, sourceTableName) #Filter for distinct sourceTables
  
  if(nrow(y) == 0){
    y <- InsertRow(y, NewRow = "no_tables") #Add note for potential error handling
    names(y) <- c("sourceTableName")
    y <- cbind(targetTableName = targetTableName, y)
  } else { 
    y <- cbind(targetTableName = targetTableName, y) #Add targetTableName
  }
  
  path <- paste(direction,procedureName[1],".csv",sep="")
  write.csv(y, file = path, row.names = FALSE)
  path_error <- paste(direction,procedureName[1],"_errorHandling",".csv",sep="")
  write.csv(df_errorHandling, file = path_error, row.names = FALSE)
  path_quality <- paste(direction,procedureName[1],"_qualityCheck",".csv",sep="")
  write.csv(df_errorHandling, file = path_quality, row.names = FALSE)
  
  y$procedureName <- procedureName
  df <- rbind(df,y)
  df_qualityCheck <- rbind(df_qualityCheck, temp_qualityCheck)
  names(df_qualityCheck) <- df_qualityCheckNames 
  path <- paste(direction,"df",".csv",sep="")
  write.csv(df, file = path, row.names = FALSE)
}
df$targetLayer <- gsub("_.*", "\\1", df[,1]) 

library('visNetwork')

# Create node list
nodes_1 <- data.frame(df$targetTableName)
names(nodes_1) <- "nodes"
nodes_2 <- data.frame(df$sourceTableName)
names(nodes_2) <- "nodes"
nodes <- rbind(nodes_1,nodes_2)
x <- table(nodes)
nodes <- data.frame(x)
names(nodes) <- c("id","frequency")
nodes$targetLayer <- gsub("_.*", "\\1", nodes[,1]) 

from <- data.frame(df$sourceTableName)
to <- data.frame(df$targetTableName)
links <- cbind(from,to)
names(links) <- c("from","to")

nodes[, 'targetLayer'] <- as.factor(nodes[, 'targetLayer'])

vis.nodes <- nodes
vis.links <- links

vis.nodes$group <- vis.nodes$targetLayer

vis.nodes$shadow <- TRUE # Nodes will drop shadow
vis.nodes$title  <- vis.nodes$id # Text on click
vis.nodes$label  <- vis.nodes$id # Node label
#vis.nodes$size   <- vis.nodes$frequency # Node size
vis.nodes$borderWidth <- 2 # Node border width

#vis.nodes$color.border <- "black"
vis.nodes$color.highlight.background <- "pink"
vis.nodes$color.highlight.border <- "darkred"

vis.links$color <- "gray"    # line color  
vis.links$arrows <- "middle" # arrows: 'from', 'to', or 'middle'
#https://html-color-codes.info/color-names/
visnet <- visNetwork(vis.nodes, vis.links) %>%
  visGroups(groupname = "rl", 
            shape = "icon", icon = list(code = "f0c0", size = 75, color = "RoyalBlue"), shadow = list(enabled = TRUE)) %>% 
  visGroups(groupname = "dm",
            shape = "icon", icon = list(code = "f1b2", size = 75, color = "SkyBlue"), shadow = list(enabled = TRUE)) %>% 
  visGroups(groupname = "cl",
            shape = "icon", icon = list(code = "f0d0", size = 75, color = "LightSteelBlue"), shadow = list(enabled = TRUE)) %>% 
  visGroups(groupname = "ods", 
            shape = "icon", icon = list(code = "f1c0", size = 75, color = "Bisque"), shadow = list(enabled = TRUE)) %>%
  visGroups(groupname = "no", 
            shape = "icon", icon = list(code = "f00d", size = 75, color = "Gray"), shadow = list(enabled = TRUE)) %>% 
  visLegend(main="Legend", position="right", ncol=1) %>%
  addFontAwesome()
visOptions(visnet, highlightNearest = TRUE, selectedBy = "targetLayer")