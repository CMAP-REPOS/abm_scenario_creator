require(data.table)
outputs_dir <- 'Y:/nmp/basic_template_20140521/model/outputs'
setwd(outputs_dir)
hh <- fread('hhData_1.csv')
tours_i <- fread('indivTourData_1.csv')
trips_i <- fread('indivTripData_1.csv')
tours_j <- fread('jointTourData_1.csv')
trips_j <- fread('jointTripData_1.csv')
