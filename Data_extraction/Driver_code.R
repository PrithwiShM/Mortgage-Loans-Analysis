
### Define the set of files to read in. PLEASE EDIT TO ACCOMODATE YOUR ANALYSIS AND INCLUDE LATER DATA RELEASES ###
starting_file <- 0 #0 starts on 2000Q1
ending_file <- 95 #95 ends on 2023Q4
setwd("~/lppub_r_code/")
#Process the files (outputs to csv)
for (file_number in starting_file:ending_file){
  
  #Set up file names
  fileYear <- file_number %/% 4
  if(nchar(fileYear) == 1){
    fileYear <- paste0('0', fileYear)
  }
  fileYear <- paste0('20', fileYear)
  fileQtr <- (file_number %% 4) + 1
  fileQtr <- paste0('Q',fileQtr)
  FileName <- paste0(fileYear, fileQtr, '.csv')
  
  #Run helper file
  source('My_data_extraction_tool.R')
}