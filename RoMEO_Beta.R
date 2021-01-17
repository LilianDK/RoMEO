##========================================================##
##                                                        ##
##   Retracing of Mapping Explicit to Objects (RoMEO)     ##
##   Purpose: Extract relevant information from SQL       ##
##   scripts such as source and target tables             ##
##                                                        ##
##   GitHub:                                              ##
##                                                        ##
##========================================================##

# ==================== Packages
# Text mining and data manipulation
library(dplyr)
library(tm)
library(quanteda)
library(DescTools)
library(DataCombine)
library(stringr)
library(stringi)
# Visualization
library('visNetwork')
# EDA
library(reshape2)
library(ggplot2)
# ==================== Set Directory
filepath <- "C:/Users/--/Desktop/"
filepath2 <- "C:/Users/--/Desktop/SQLS/pipe_2_scripts"
direction <- paste(filepath, "SQLS/output/", sep="")

files <- list.files(path=filepath2, pattern="*.txt", full.names = TRUE, recursive=FALSE)
# -------------------- Prepare final result, quality check and error handling tables for validation purposes
df <- data.frame(targetTableName=as.character(), sourceTableName=as.character())
df_qualityCheck <- data.frame(procedure=as.character(), countOfInsert=as.character())
df_qualityCheckNames <- names(df_qualityCheck)

# ==================== Extraction of Data
for (i in files) {
  ### for each sql file an error handling table
  df_errorHandling <- data.frame(romeoStep=as.character(), rawOutput=as.character())
  df_errorHandlingNames <- names(df_errorHandling)
  
  # -------------------- Text mining preparation
  createCorpus <- function(filepath2) {
    conn <- file(filepath2, "r")
    fulltext <- readLines(conn)
    close(conn)
    
    vs <- VectorSource(fulltext)
    Corpus(vs, readerControl=list(readPlain, language="en", load=TRUE))
  }
  
  rawCorpus <- createCorpus(i)
  analysisCorpus <- corpus(rawCorpus)
  
  # START OF TEXT MINING INTELLIGENCE
  # -------------------- Text Mining: Extraction of PROCEDURE NAME identified by "procedure"-prefix per line
  procedureName <- corpus_segment(analysisCorpus, pattern = "procedure*") 
  df_temp <- data.frame("1 Extract Pattern of Procedure",procedureName)
  names(df_temp) <- df_errorHandlingNames
  df_errorHandling <- rbind(df_errorHandling, df_temp)
  names(df_errorHandling) <- df_errorHandlingNames
  procedureName <- gsub(" .*","",procedureName)
  procedureName <- procedureName[1]
  procedureName
  # -------------------- Text Mining: Extraction of TARGET TABLE NAME identified by "insert into"-prefix per line
  insertInto <- corpus_segment(analysisCorpus, pattern = "insert into*") 
  insertInto <- data.frame(insertInto)
  ## ------------------- If there is no valid target table identifiable then noted as "no_target"
  if(nrow(insertInto) == 0){
    insertInto <- "no_target"
  } else { 
  }
  df_temp <- data.frame("2 Extract Pattern of Insert Into",insertInto)
  names(df_temp) <- df_errorHandlingNames
  df_errorHandling <- rbind(df_errorHandling, df_temp)
  insertInto <- data.frame(insertInto)
  ## ------------------- Target table identification rule 1: "#" and "@" do not represent valid table names
  insertInto <- insertInto[grepl("_", insertInto$insertInto),] 
  insertInto <- gsub("[;]","",insertInto) # Remove ;
  insertInto <- data.frame(insertInto)
  insertInto$insertInto <- gsub(" .*", "\\1", insertInto[,1]) # Take first word
  insertInto$cleanPrep <- StrPos(insertInto[,1], "_", pos = 1)
  insertInto <- insertInto[!grepl("#|@", insertInto$insertInto),] 
  insertInto <- data.frame(insertInto)
  insertInto <- distinct(insertInto)
  insertInto <- insertInto[,1]
  count_temp <- length(insertInto)
  temp_qualityCheck <- data.frame(procedureName, count_temp)
  names(temp_qualityCheck) <- df_qualityCheckNames
  ## ------------------- If there is no valid target table identifiable then noted as "no_target"
  if(length(insertInto) == 1){
  } else { 
    insertInto <- "no_target"
  }
  targetTableName <- gsub(" .*","",insertInto)
  targetTableName
  # -------------------- Text Mining: Extraction of SOURCE TABLE NAME identified by "from"-prefix per line
  x <- corpus_segment(analysisCorpus, pattern = "from*") 
  y <- data.frame(x)
  if(nrow(y) == 0){
    y <- "no_table"
  } else { 
  }
  df_temp <- data.frame("3 Extraction of Source Tables",x)
  names(df_temp) <- df_errorHandlingNames
  df_errorHandling <- rbind(df_errorHandling, df_temp)
  ## ------------------- Source table identification rule 1: "#" and "@" do not represent valid table names
  y <- y[grepl("_", y$x),] 
  y <- gsub("[;']","",y) # Remove ;
  y <- gsub("\\[|\\]", "", y) # Remove brackets
  y <- str_remove_all(y, "[()]") # Remove round brackets
  y <- data.frame(y)
  y$sourceTableName <- gsub(" .*", "\\1", y[,1]) # Take first word
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
  y
  # -------------------- Text Mining: Extraction of SOURCE TABLE NAME identified by "join"-prefix per line
  x <- corpus_segment(analysisCorpus, pattern = "join*") 
  y2 <- data.frame(x)
  if(nrow(y2) == 0){
    x <- "no_table"
  } else { 
  }
  df_temp <- data.frame("3 Extraction of Source Tables",x)
  names(df_temp) <- df_errorHandlingNames
  df_errorHandling <- rbind(df_errorHandling, df_temp)
  ## ------------------- Source table identification rule 1: "#" and "@" do not represent valid table names
  y2 <- y2[grepl("_", y2$x),] 
  y2 <- gsub("[;']","",y2) # Remove ;
  y2 <- gsub("\\[|\\]", "", y2) # Remove brackets
  y2 <- str_remove_all(y2, "[()]") # Remove round brackets
  y2 <- data.frame(y2)
  y2$sourceTableName <- gsub(" .*", "\\1", y2[,1]) # Take first word
  y2 <- y2[!grepl("#|@", y2$y2),] #Filter all expressions which do not contain #,@
  y2 <- distinct(y2, sourceTableName) #Filter for distinct sourceTables
  y2 <- y2[grepl("_", y2$sourceTableName),]
  y2 <- data.frame(y2)
  names(y2) <- "sourceTableName"
  ## ------------------- If there is no valid source table identifiable then noted as ""
  if(nrow(y2) == 0){
  } else { 
    y2 <- cbind(targetTableName = targetTableName, y2) 
  }
  y2
  # END OF TEXT MINING INTELLIGENCE
  
  # -------------------- Outputs: Make various output tables
  path <- paste(direction,procedureName[1],".csv",sep="")
  y <- rbind(y,y2)
  write.csv(y, file = path, row.names = FALSE)
  path_error <- paste(direction,procedureName[1],"_errorHandling",".csv",sep="")
  write.csv(df_errorHandling, file = path_error, row.names = FALSE)
  path_quality <- paste(direction,procedureName[1],"_qualityCheck",".csv",sep="")
  write.csv(df_errorHandling, file = path_quality, row.names = FALSE)
  
  y$procedureName <- as.character(procedureName)
  df <- rbind(df,y)
  df_qualityCheck <- rbind(df_qualityCheck, temp_qualityCheck)
  names(df_qualityCheck) <- df_qualityCheckNames 
  df
  path <- paste(direction,"df",".csv",sep="")
  write.csv(df, file = path, row.names = FALSE)
}

# ==================== Visualization of Data
# -------------------- Data preparation for required input format network visualization
## -------------------- Data preparation: Substring of sourceTableNames containing "ods", e.g. common.ods_xyz to ods_xyz
df[, 'targetTableName'] <- as.character(df[, 'targetTableName'])
df[, 'sourceTableName'] <- as.character(df[, 'sourceTableName'])
df$sourceTableName2 <- substr(df$sourceTableName, regexpr("ods", df$sourceTableName), nchar(df$sourceTableName))
## -------------------- Data preparation: Substring prefix
df$targetLayer <- gsub("_.*", "\\1", df[,1]) 
df$sourceLayer <- gsub("_.*", "\\1", df[,2]) 
## -------------------- Data preparation: Hierarchy preparation
tmp <- ifelse(is.na(str_locate(df$sourceTableName2, "\\.")) , df$sourceTableName2, unlist(strsplit(df$sourceTableName2, "\\.")))
tmp <- matrix(tmp , ncol = 2 , byrow = TRUE)
tmp <- ifelse(str_detect(tmp[,1], "ods") == TRUE, 
              sub('_([^_]*)$', '', tmp[,1]), 
              gsub("_.*", "\\1", tmp[,1]))
tmp <- data.frame(tmp)
names(tmp) <- "sourceTableName3"
#unique.tmp <- unique(tmp)
#unique.targetLayer <- data.frame(unique(df$targetLayer))
df <- cbind(df, tmp)
df[, 'sourceTableName3'] <- as.character(df[, 'sourceTableName3'])

prep1 <- df$targetTableName
prep2 <- ifelse(gsub("_.*", "\\1", prep1) == "rl",prep1,gsub("_.*", "\\1", prep1))
prep2 <- data.frame(prep2)
names(prep2) <- "targetLayer2"
df <- cbind(df, prep2)
df[, 'targetLayer2'] <- as.character(df[, 'targetLayer2'])

#write.csv(df, file = path, row.names = FALSE)
## ------------------- Nodes
### -- Version 1
#nodes_1 <- data.frame(df$targetLayer2)
nodes_1 <- data.frame(df$targetTableName)
names(nodes_1) <- c("nodes_v1")
#nodes_2 <- data.frame(df$sourceTableName3)
nodes_2 <- data.frame(df$sourceTableName)
names(nodes_2) <- c("nodes_v1")
nodes <- rbind(nodes_1,nodes_2)
x <- table(nodes)
nodes_v1 <- data.frame(x)
names(nodes_v1) <- c("id","frequency")

nodes_v1[,1] <- gsub("common.","",nodes_v1[,1])
nodes_v1[,1] <- gsub("hosting.","",nodes_v1[,1])
nodes_v1[,1] <- gsub("access.","",nodes_v1[,1])
nodes_v1$targetLayer <- gsub("_.*", "\\1", nodes_v1[,1])
nodes_v1[, 'targetLayer'] <- as.factor(nodes_v1[, 'targetLayer'])
### -- Version 2
nodes_3 <- data.frame(df$targetTableName)
nodes_3 <- Map(paste, 'cube_', nodes_3)
nodes_3 <- data.frame(nodes_3)
names(nodes_3) <- c("nodes_v2")
nodes_4 <- data.frame(df$sourceTableName)
names(nodes_4) <- c("nodes_v2")
nodes <- rbind(nodes_3,nodes_4)
x <- table(nodes)
nodes_v2 <- data.frame(x)
names(nodes_v2) <- c("id","frequency")

nodes_v2[,1] <- gsub("hosting.","",nodes_v2[,1])
nodes_v2[,1] <- gsub("\\bproms.\\b","ods_proms.",nodes_v2[,1])
nodes_v2$targetLayer <- gsub("_.*", "\\1", nodes_v2[,1])
nodes_v2[, 'targetLayer'] <- as.factor(nodes_v2[, 'targetLayer'])
## ------------------- Links
### Version 1
#from_1 <- data.frame(df$sourceTableName3)
from_1 <- data.frame(df$sourceTableName)
#to_1 <- data.frame(df$targetLayer2)
to_1 <- data.frame(df$targetTableName)
links_v1 <- cbind(from_1,to_1)
names(links_v1) <- c("from","to")
links_v1[,1] <- gsub("common.","",links_v1[,1])
links_v1[,1] <- gsub("hosting.","",links_v1[,1])
links_v1[,1] <- gsub("access.","",links_v1[,1])
### Version 2
from_2 <- data.frame(df$sourceTableName)
to_2 <- data.frame(df$targetTableName)
to_2 <- Map(paste, 'cube_', to_2)
to_2 <- data.frame(to_2)
links_v2 <- cbind(from_2,to_2)
names(links_v2) <- c("from","to")
links_v2[,1] <- gsub("hosting.","",links_v2[,1])
links_v2[,1] <- gsub("\\bproms.\\b","ods_proms.",links_v2[,1])
# -------------------- Visualization type 1.1
vis.nodes_1 <- nodes_v1
vis.links_1 <- links_v1

vis.links_1<- vis.links_1 %>%
  filter(#vis.links_1$from == "cl" | 
    #vis.links_1$from == "dm"| 
    #vis.links_1$from == "rl"|
    str_detect(vis.links_1$from, "ods") == TRUE)

vis.nodes_1$group <- vis.nodes_1$targetLayer

vis.nodes_1$shadow <- TRUE # Nodes will drop shadow
vis.nodes_1$title  <- vis.nodes_1$id # Text on click
vis.nodes_1$label  <- vis.nodes_1$id # Node label
#vis.nodes_1$size   <- vis.nodes_1$frequency # Node size
vis.nodes_1$borderWidth <- 2 # Node border width

#vis.nodes_1$color.border <- "black"
vis.nodes_1$color.highlight.background <- "pink"
vis.nodes_1$color.highlight.border <- "darkred"

vis.links_1$color <- "gray"    # line color  
vis.links_1$arrows <- "middle" # arrows: 'from', 'to', or 'middle'

vis.nodes_1 <- vis.nodes_1[!duplicated(vis.nodes_1$id), ]

#https://html-color-codes.info/color-names/
visnet <- visNetwork(vis.nodes_1, vis.links_1, width = "100%", height = 1000) %>%
  visGroups(groupname = "rl", 
            shape = "icon", icon = list(code = "f0c0", size = 75, color = "SkyBlue"), shadow = list(enabled = TRUE)) %>% 
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

# -------------------- Visualization type 1.2
vis.nodes_2 <- nodes_v2
vis.links_2 <- links_v2

vis.nodes_2$group <- vis.nodes_2$targetLayer

vis.nodes_2$shadow <- TRUE # Nodes will drop shadow
vis.nodes_2$title  <- vis.nodes_2$id # Text on click
vis.nodes_2$label  <- vis.nodes_2$id # Node label
#vis.nodes_2$size   <- vis.nodes_2$frequency # Node size
vis.nodes_2$borderWidth <- 2 # Node border width

#vis.nodes_2$color.border <- "black"
vis.nodes_2$color.highlight.background <- "pink"
vis.nodes_2$color.highlight.border <- "darkred"

vis.links_2$color <- "black"    # line color  
vis.links_2$arrows <- "middle" # arrows: 'from', 'to', or 'middle'
#https://html-color-codes.info/color-names/
visnet <- visNetwork(vis.nodes_2, vis.links_2, width = "100%", height = 1000) %>%
  visGroups(groupname = "cube", 
            shape = "icon", icon = list(code = "f0c0", size = 75, color = "RoyalBlue"), shadow = list(enabled = TRUE)) %>% 
  visGroups(groupname = "rl",
            shape = "icon", icon = list(code = "f0c0", size = 45, color = "SkyBlue"), shadow = list(enabled = TRUE)) %>% 
  visGroups(groupname = "dm",
            shape = "icon", icon = list(code = "f111", size = 20, color = "ForestGreen"), shadow = list(enabled = TRUE)) %>% 
  visGroups(groupname = "cl",
            shape = "icon", icon = list(code = "f111", size = 20, color = "Tomato"), shadow = list(enabled = TRUE)) %>% 
  visGroups(groupname = "ods", 
            shape = "icon", icon = list(code = "f1c0", size = 75, color = "Bisque"), shadow = list(enabled = TRUE)) %>%
  visGroups(groupname = "no", 
            shape = "icon", icon = list(code = "f00d", size = 75, color = "Gray"), shadow = list(enabled = TRUE)) %>% 
  visLegend(main="Legend", position="right", ncol=1) %>%
  addFontAwesome()
visOptions(visnet, highlightNearest = TRUE, selectedBy = "targetLayer")

# -------------------- Visualization type total
vis.nodes <- rbind(vis.nodes_1, vis.nodes_2)
vis.links <- rbind(vis.links_1, vis.links_2)

vis.nodes <- vis.nodes[!duplicated(vis.nodes$id), ]

#https://html-color-codes.info/color-names/
visnet <- visNetwork(vis.nodes, vis.links, width = "100%", height = 1000) %>%
  visGroups(groupname = "cube", 
            shape = "icon", icon = list(code = "f0c0", size = 75, color = "RoyalBlue"), shadow = list(enabled = TRUE)) %>% 
  visGroups(groupname = "rl",
            shape = "icon", icon = list(code = "f0c0", size = 45, color = "SkyBlue"), shadow = list(enabled = TRUE)) %>% 
  visGroups(groupname = "dm",
            shape = "icon", icon = list(code = "f111", size = 20, color = "ForestGreen"), shadow = list(enabled = TRUE)) %>% 
  visGroups(groupname = "cl",
            shape = "icon", icon = list(code = "f111", size = 20, color = "Tomato"), shadow = list(enabled = TRUE)) %>% 
  visGroups(groupname = "ods", 
            shape = "icon", icon = list(code = "f1c0", size = 75, color = "Bisque"), shadow = list(enabled = TRUE)) %>%
  visGroups(groupname = "no", 
            shape = "icon", icon = list(code = "f00d", size = 75, color = "Gray"), shadow = list(enabled = TRUE)) %>% 
  visLegend(main="Legend", position="right", ncol=1) %>%
  addFontAwesome()
visOptions(visnet, highlightNearest = TRUE, selectedBy = "targetLayer")

nodes <- data.frame(id = 1:3)
edges <- data.frame(from = c(1,2), to = c(1,3), value = c(1:2))

edges$color <- palette()[edges$value]

visNetwork(nodes, edges)
# -------------------- Visualization type 2
library(readr)
romeo_hierarchy <- read_delim("C:/Users/DOKHAL/Desktop/SQLS/config/romeo_hierarchy.csv", 
                              ";", escape_double = FALSE, trim_ws = TRUE)
View(romeo_hierarchy)
df2 <- cbind(df$sourceTableName3, df$targetLayer)
names(df2) <- c("from","to")
hierarchy <- rbind(romeo_hierarchy , df2)
romeo.vertices <- data.frame(name = unique(c(as.character(hierarchy$from), as.character(hierarchy$to))) )
mygraph <- graph_from_data_frame( hierarchy, vertices=romeo.vertices )
library(ggraph)
library(igraph)
# create a data frame giving the hierarchical structure of your individuals. 
# Origin on top, then groups, then subgroups
d1 <- data.frame(from="origin", to=paste("group", seq(1,10), sep=""))
d2 <- data.frame(from=rep(d1$to, each=10), to=paste("subgroup", seq(1,100), sep="_"))
hierarchy <- rbind(d1, d2)
# create a vertices data.frame. One line per object of our hierarchy, giving features of nodes.
vertices <- data.frame(name = unique(c(as.character(hierarchy$from), as.character(hierarchy$to))) ) 
# Create a graph object with the igraph library
mygraph <- graph_from_data_frame( hierarchy, vertices=vertices )
# This is a network object, you visualize it as a network like shown in the network section!
# With igraph: 
plot(mygraph, vertex.label="", edge.arrow.size=0, vertex.size=2)
# With ggraph:
ggraph(mygraph, layout = 'dendrogram', circular = FALSE) + 
  geom_edge_link() +
  theme_void() 
ggraph(mygraph, layout = 'dendrogram', circular = TRUE) + 
  geom_edge_diagonal() +
  theme_void()
# -------------------- GAMMEL 
library('igraph')

net <- graph_from_data_frame(d=links, vertices=nodes, directed=T) 
net
plot(net, layout=layout_in_circle)

library(threejs)
library(htmlwidgets)
library(igraph)
gjs <- graphjs(net, main="Network!", bg="gray10", showLabels=TRUE, stroke=F, 
               curvature=0.1, attraction=0.9, repulsion=0.8, opacity=0.9)
print(gjs)
saveWidget(gjs, file="Media-Network-gjs.html")
browseURL("Media-Network-gjs.html")

library('visNetwork')
vis.nodes <- nodes
vis.links <- links
vis.nodes$label  <- vis.nodes$id # Node label
visNetwork(vis.nodes, vis.links)

# ==================== EDA
dtm <- DocumentTermMatrix(analysisCorpus)
dtm.matrix <- as.matrix(dtm)
wordcount <- colSums(dtm.matrix)
topten <- head(sort(wordcount, decreasing=TRUE), 20)

dfplot <- as.data.frame(melt(topten))
dfplot$word <- dimnames(dfplot)[[1]]
dfplot$word <- factor(dfplot$word,
                      levels=dfplot$word[order(dfplot$value,
                                               decreasing=TRUE)])

fig <- ggplot(dfplot, aes(x=word, y=value)) + geom_bar(stat="identity")
fig <- fig + xlab("Word in Corpus")
fig <- fig + ylab("Count")
print(fig)