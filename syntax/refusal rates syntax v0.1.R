# SYNTAX NOTES ----
#OBJECTIVE:  
#DATE FIRST CREATED 9/13/21
#CREATED BY: ALLAN URIBE 

#PACKAGES NEEDED----
install.packages("easypackages")
library('easypackages')

install_packages(  'DBI', 'odbc'
                 , 'summarytools', 'tidyverse', 'lubridate'
                 , 'sqldf', 'data.table','IndexNumR')

libraries(  'DBI', 'odbc'
            , 'summarytools', 'tidyverse', 'lubridate'
            , 'sqldf', 'data.table','IndexNumR')
options(scipen = 30) # dont display long numbers in R as exponents

# CONNECTIONS TO THE DATASEVERS USED IN THIS PROGRAM----
# Connection to the Westat server
westat_con <- dbConnect(
    odbc(),
    Driver = "ODBC Driver 17 for SQL Server",
    Server = "158.111.202.203",
    Database = "MSDELIVR_db",
    UID = "NCHSDASH",
    PWD = rstudioapi::askForPassword("Database password")
)

con <- dbConnect(
    odbc(),
    Driver = "SQL Server",
    Server = "DSPP-HANE-1601",
    Database = "ANL1_db",
    Trusted_Connection = "True",
    UID = "qsj2",
    Port = 1433
)
# FUNCTIONS USED IN THIS CODE----
# create a function that negates the %in% function in R which is not included
#notin----
'%notin%' <- Negate('%in%')

needed_stands <- c(284, 287, 291 ,297, 299, 304, 309, 312
                   ,319, 329, 331, 333, 337, 340, 343, 345
                   ,350, 353, 355, 360, 365, 366, 369, 371
                   ,372, 374, 376, 378, 379, 380, 384, 386
                   ,387, 388, 389, 394, 396, 398, 403, 407
                   ,412, 428, 429, 430, 431, 432, 433, 434
                   ,435, 436, 437, 438, 439, 440, 441, 442
                   ,695)
 

# list of variables to keep when we import
ri7_var_keep <- c(
    #     "convert(varchar,SCACASE) as caseid"
      "SCACASE as caseid"
    , "SDASTAND as standid"
    , "SDASEGMT as segmentid"
    , "SDASERAL as dwellingid"
    , "STAQTYP as case_type"
    , "SCAFAMNO as familyid"        
    , "SCAPERSN as personid"
    , "STARSPNO as participantid"
    , "AAASESSN as mec_session_id"
    , "STAINTDT as case_date"
    , "RIQ004 as interview_mode"
    , "RIAPAPMD as paper_mode"
    , "RIAPHNMD as Phone_mode"
   
)

# create R object with the SQL query
screener_ri7_query <- paste(
    "SELECT ", paste(ri7_var_keep, sep = "", collapse= ", "),
    " FROM ANL_RI7_SAM_Case
      WHERE STAQTYP in(1,2) and SDASTAND in(", paste(needed_stands,sep= ,collapse=","),") ")# selecting only the screener and relationship cases d

# see what the sql query looks like 
cat(screener_ri7_query)

screener_ri7 <- dbGetQuery(westat_con, screener_ri7_query)

curr_case_keep <- c(
    "case_id as caseid_cc"
    , "case_dt case_date_cc"
    , "question_disp as case_disposition_cc"
    , "emp_id as employee_id_cc"
    , "interview_mode as interview_mode_cc"
    , "Paper_mode as paper_mode_cc"
    , "Phone_mode as Phone_mode_cc"
    , "Mode_text as mode_text_cc"
)
screener_curr_case_query <- paste(
    "SELECT ", paste(
        curr_case_keep, sep = "",collapse= ","),
    "FROM V_Current_Case WHERE case_id IN (SELECT SCACASE FROM ANL_RI7_SAM_Case WHERE STAQTYP in(1,2) and SDASTAND in(", paste(needed_stands,sep= ,collapse=","),"))")

cat(screener_curr_case_query)
screener_curr_case <- dbGetQuery(westat_con, screener_curr_case_query)

# Pull in DU release status ----
du_released_status_keep <- c("*")

du_released_status_query <- paste(
    "SELECT ", paste(
        du_released_status_keep, sep ="" ,collapse= ","),
    "FROM v_anl_du_released_status WHERE SDASTAND in (", paste(needed_stands,sep= ,collapse=","),")")

cat(du_released_status_query)
du_released_status <- dbGetQuery(con, du_released_status_query) #COMbak  this table is not on the westat server

#combak Consider changing var names in the sql statement  
du_released_status<-du_released_status %>% rename(standid = SDASTAND)
du_released_status<-du_released_status %>% rename(segmentid = SDASEGMT)
du_released_status<-du_released_status %>% rename(dwellingid = SDASERAL)
du_released_status<-du_released_status %>% rename(release_status = SCADURST)

####----





person_ri1_keep <- c(
    "SP_ID as spid"
    , "SDASTAND as standid_p"
    , "SDASEGMT as segmentid_p"
    , "SDASERAL as dwellingid_p"
    , "SCAFAMNO as familyid_p"
    , "SCAPERSN as personid_p"
    , "STAINTDT as interview_date_p"
    , "RIAPCODE as person_code_p"
    , "SCQCK305 as SP_selection_status_p"
)
person_ri1_query <- paste(
    "SELECT ", paste(
        person_ri1_keep, sep = "",collapse= ","),
    "FROM VN_ANL_RI1_NH_Person where SDASTAND in(", paste(needed_stands,sep= ,collapse=","),")")

cat(person_ri1_query)
person_ri1 <- dbGetQuery(westat_con, person_ri1_query)



NH_Appointment_keep <- c(
    "appt_ID"
    , "SP_ID"
    , "appt_type"
    , "stand_ID"
    , "appt_status"
    , "last_updated_empid"
    , "status_DT"
    , "appt_arrival_DT"
)

NH_Appointment_query <- paste(
    "SELECT ", paste(
        NH_Appointment_keep, sep = "",collapse= ","),
    "FROM V_NH_Appointment where stand_ID in(", paste(needed_stands,sep= ,collapse=","),")")

cat(NH_Appointment_query)
NH_Appointment <- dbGetQuery(westat_con, NH_Appointment_query)

# Join the case table with the current case table to get the dispositions and more





# CREATE ALL THE KEYS NEEDED ON EACH TABLE ----
# screener case fact keys ----

# for joining at the segment level
screener_ri7$segment_key <- paste(
    screener_ri7$standid
    ,screener_ri7$segmentid
    ,sep = "_"
)

# form joining at the dwelling level 
screener_ri7$dwelling_key <- paste(
    screener_ri7$standid
    ,screener_ri7$segmentid
    ,screener_ri7$dwellingid, sep = "_"
)

# this var can be used to join to screener records. Family is left out b/c that is not 
#'identified until later in the survey.
#'for joining at the participant level.    
screener_ri7$participant_key <- paste(
    screener_ri7$standid
    ,screener_ri7$segmentid
    ,screener_ri7$dwellingid
    ,screener_ri7$participantid
    ,sep = "_"
)

screener_ri7$person_key <- paste(
    screener_ri7$standid
    ,screener_ri7$segmentid
    ,screener_ri7$dwellingid
    ,screener_ri7$familyid
    ,screener_ri7$personid
    ,sep = "_"
)


screener_case_fact <- sqldf('SELECT A.* , B.case_date_cc
                                        , B.case_disposition_cc
                                        , B.employee_id_cc
                                        , B.interview_mode_cc
                                        , B.paper_mode_cc
                                        , B.Phone_mode_cc
                                        , B.mode_text_cc
                             FROM screener_ri7 as A 
                             LEFT JOIN screener_curr_case as B
                             ON A.caseid=B.caseid_cc')


#released dwelling keys----
# Creating join vars at various levels in the stat_DU
# for joining at the segment level
du_released_status$segment_key <- paste(
    du_released_status$standid
    ,du_released_status$segmentid
    ,sep = "_"
)

# form joining at the dwelling level 
du_released_status$dwelling_key <- paste(
    du_released_status$standid
    ,du_released_status$segmentid
    ,du_released_status$dwellingid, sep = "_"
)

screener_case_fact <- sqldf(('SELECT A.* 
                   , B.release_status
                   FROM screener_case_fact as A 
                   LEFT JOIN du_released_status as B
                   ON A.dwelling_key=B.dwelling_key'),drv="SQLite")


screener_case_fact$released_dwelling <- 
    ifelse((screener_case_fact$release_status =="1" & screener_case_fact$case_type==1)
           ,1,0)
screener_case_fact$fi_assigned_screener <- 
    ifelse((screener_case_fact$case_type==1)
           &(screener_case_fact$released_dwelling==1)
           & (screener_case_fact$employee_id > 0),1,0)

screener_case_fact$completed_screener <- with(
    screener_case_fact, 
    ifelse((case_type==1)
           & (fi_assigned_screener == 1)
           & (case_disposition_cc %in% c('10','11','12','13')),1,
           ifelse((case_type==1)
                  & (fi_assigned_screener == 1)
                  & (case_disposition_cc %notin% c('10','11','12','13')),0,NA)))

screener_case_dup <- subset(screener_case_fact, standid %in% c(319,369,384,394, 398,403,407))

screener_case_dup$standid <- with(screener_case_dup,
  ifelse(
    standid==319,'319_2',
    ifelse(
      standid==369,'369_2',
      ifelse(
        standid==398, '398_2',
        ifelse(
          standid==403, '403_2',
          ifelse(
            standid==384, '384_2',
            ifelse(
              standid==394, '394_2',
              ifelse(
                standid==407, '407_2',0
              )
            )
          )
        )
      )
    )
  )
)
screener_case_fact$standid <-as.character(screener_case_fact$standid)

screener_case_fact<-screener_case_fact[,-29]
screener_case_dup<-screener_case_dup[,-29]

screener_case_fact1<-rbind(screener_case_fact,screener_case_dup)

Completed_Screening_forcast <- screener_case_fact1 %>% 
    group_by(standid,date_screener=date(case_date_cc))%>%
    summarise(N_aggragated_obs_screener = n(),
              number_of_complete_screenings =sum(completed_screener,na.rm=T)) %>%
  mutate (cumulative_sum_complete_screenings=(cumsum(number_of_complete_screenings)))


'695'<- subset(Completed_Screening_forcast,standid==695)

freq(screener_case_fact$completed_screener[which(screener_case_fact$standid==695)])


Completed_Screening_forcast$associated_stand <- with(
  Completed_Screening_forcast, 
  ifelse(
    standid %in% c(428, 345, 407, 331),428,
    ifelse(
      standid %in% c(429, 369, 365, 304),429,
      ifelse(
        standid %in% c(430, 284, 333, 299),430,
        ifelse(
          standid %in% c(431, 412, '394_2', 343),431,
          ifelse(
            standid %in% c(432, 378, 396, 394),432,
            ifelse(
              standid %in% c(433, '369_2', 371, 366),433,
              ifelse(
                standid %in% c(434, '398_2', 355),434,
                ifelse(
                  standid %in% c(435, 398, 337, 340, 309),435,
                  ifelse(
                    standid %in% c(436, '384_2', 380, '319_2'),436,
                    ifelse(
                      standid %in% c(437, 386, 387, 319),437,
                      ifelse(
                        standid %in% c(438, 353, 291, 384),438,
                        ifelse(
                          standid %in% c(439, 360, 389, 312),439,
                          ifelse(
                            standid %in% c(440, 350, 287, 379),440,
                            ifelse(
                              standid %in% c(441, '403_2', 376, 329),441,
                              ifelse(
                                standid %in% c(442, 403, 388, 297),442,
                                ifelse(
                                  standid %in% c(695, 372, 374, '407_2'),695,0
                                )))))))))))))))))

class(Completed_Screening_forcast$date_screener)

Completed_Screening_forcast <- Completed_Screening_forcast %>% drop_na(date_screener)

Completed_Screening_forcast1 <-  Completed_Screening_forcast %>%
  mutate(date_screener) %>%
  complete(date_screener = seq.Date(min(date_screener), max(date_screener), by="day")) %>%
  group_by (as.character(standid)) %>%
  fill(N_aggragated_obs_screener, number_of_complete_screenings, cumulative_sum_complete_screenings, associated_stand)



Completed_Screening_forcast1 <-Completed_Screening_forcast1 %>%
  group_by (as.character(standid)) %>%
  mutate(day= row_number()-1)

Completed_Screening_forcast1$week <- week(Completed_Screening_forcast1$date_screener)

Completed_Screening_forcast1 <- Completed_Screening_forcast1 %>% 
  group_by (as.character(standid)) %>%
  mutate(week_index = (year(date_screener) - year(min(date_screener)))*52 + 
           week(date_screener) - week(min(date_screener)))

Completed_Screening_forcast1 <- Completed_Screening_forcast1 %>% 
  group_by (as.character(standid)) %>%
  mutate(month_index = (year(date_screener) - year(min(date_screener)))*12 + 
           month(date_screener) - month(min(date_screener)))



# Join the case table with the current case table to get the dispositions and more





# CREATE ALL THE KEYS NEEDED ON EACH TABLE ----
# screener case fact keys ----

# create R object with the SQL query
SPint_ri7_query <- paste(
    "SELECT ", paste(ri7_var_keep, sep = "", collapse= ", "),
    " FROM ANL_RI7_SAM_Case
      WHERE STAQTYP in(4) and SDASTAND in(", paste(needed_stands,sep= ,collapse=","),") ")# selecting only the screener and relationship cases d

# see what the sql query looks like 
cat(SPint_ri7_query)

SPint_ri7 <- dbGetQuery(westat_con, SPint_ri7_query)


SPint_curr_case_query <- paste(
    "SELECT ", paste(
        curr_case_keep, sep = "",collapse= ","),
    "FROM V_Current_Case WHERE case_id IN (SELECT SCACASE FROM ANL_RI7_SAM_Case WHERE STAQTYP in(4) and SDASTAND in(", paste(needed_stands,sep= ,collapse=","),"))")

cat(SPint_curr_case_query)
SPint_curr_case <- dbGetQuery(westat_con, SPint_curr_case_query)


# for joining at the segment level
SPint_ri7$segment_key <- paste(
    SPint_ri7$standid
    ,SPint_ri7$segmentid
    ,sep = "_"
)

# form joining at the dwelling level 
SPint_ri7$dwelling_key <- paste(
    SPint_ri7$standid
    ,SPint_ri7$segmentid
    ,SPint_ri7$dwellingid, sep = "_"
)

# this var can be used to join to screener records. Family is left out b/c that is not 
#'identified until later in the survey.
#'for joining at the participant level.    
SPint_ri7$participant_key <- paste(
    SPint_ri7$standid
    ,SPint_ri7$segmentid
    ,SPint_ri7$dwellingid
    ,SPint_ri7$participantid
    ,sep = "_"
)

SPint_ri7$person_key <- paste(
    SPint_ri7$standid
    ,SPint_ri7$segmentid
    ,SPint_ri7$dwellingid
    ,SPint_ri7$familyid
    ,SPint_ri7$personid
    ,sep = "_"
)



SPint_case_fact <- sqldf('SELECT A.* , B.*
                             FROM SPint_ri7 as A 
                             LEFT JOIN SPint_curr_case as B
                             ON A.caseid=B.caseid_cc')

SPint_case_fact <- sqldf(('SELECT A.* 
                   , B.release_status
                   FROM SPint_case_fact as A 
                   LEFT JOIN du_released_status as B
                   ON A.dwelling_key=B.dwelling_key'),drv="SQLite")


person_ri1$person_key <- paste(
    person_ri1$standid
    ,person_ri1$segmentid
    ,person_ri1$dwellingid
    ,person_ri1$familyid
    ,person_ri1$personid
    ,sep = "_"
)

SPint_case_fact <- sqldf(('SELECT A.* 
                   , B.*
                   FROM SPint_case_fact as A 
                   LEFT JOIN person_ri1 as B
                   ON A.person_key=B.person_key'),drv="SQLite")



SPint_case_fact$completed_sp <- with(
    SPint_case_fact, 
    ifelse(SP_selection_status_p %in% c(1,4)
           & case_type ==4
           & case_disposition_cc %in% c(14),1,
    ifelse(SP_selection_status_p %in% c(1,4)
                  & case_type ==4
                  & case_disposition_cc %notin% c(14),0,NA)))

SPint_case_fact <- subset(SPint_case_fact, select = -c(36))

SPint_case_fact1 <- subset(SPint_case_fact,completed_sp==1 )

Completed_spint_forcast <- SPint_case_fact1 %>% 
    group_by(standid,date_SPint=date(SPint_case_fact1$case_date_cc))%>%
    summarise(N_aggragated_obs_SPint = n(),
              number_of_complete_sp =sum(completed_sp,na.rm=T)) %>%
    mutate (cumulative_sum_complete_sp=(cumsum(number_of_complete_sp)))
#####

MEC_completes <- sqldf(
    "SELECT A.*, B.* 
    FROM person_ri1 as A 
    LEFT JOIN NH_Appointment as B
    ON A.spid = B.SP_ID"
)

MEC_completes$MEC_examined <- with(MEC_completes,
                                   ifelse(
                                       SP_selection_status_p %in% c(1,4)
                                       & person_code_p == 2 
                                       & appt_status == 2
                                       & appt_type == 1,1,0
                                   ))

MEC_ri7_query <- paste(
  "SELECT ", paste(ri7_var_keep, sep = "", collapse= ", "),
  " FROM ANL_RI7_SAM_Case
      WHERE STAQTYP in(5) and SDASTAND in(", paste(needed_stands,sep= ,collapse=","),") ")# selecting only the screener and relationship cases d

# see what the sql query looks like 
cat(MEC_ri7_query)

MEC_ri7 <- dbGetQuery(westat_con, MEC_ri7_query)


MEC_curr_case_query <- paste(
  "SELECT ", paste(
    curr_case_keep, sep = "",collapse= ","),
  "FROM V_Current_Case WHERE case_id IN (SELECT SCACASE FROM ANL_RI7_SAM_Case WHERE STAQTYP in(5) and SDASTAND in(", paste(needed_stands,sep= ,collapse=","),"))")

cat(MEC_curr_case_query)
MEC_curr_case <- dbGetQuery(westat_con, SPint_curr_case_query)

MEC_ri7$person_key <- paste(
  MEC_ri7$standid
  ,MEC_ri7$segmentid
  ,MEC_ri7$dwellingid
  ,MEC_ri7$familyid
  ,MEC_ri7$personid
  ,sep = "_"
)

MEC_date_keep <- c(
    "SP_ID"
  , "SP_Checkin_Time"
)

MEC_date_query <- paste(
  "SELECT ", paste(
    MEC_date_keep, sep = "",collapse= ","),
  "FROM MEC_Exam_Sum WHERE SP_ID IN (SELECT sp_id FROM VN_ANL_RI1_NH_Person WHERE SDASTAND 
  in(", paste(needed_stands,sep= ,collapse=","),"))") 

cat(MEC_date_query)

MEC_date <- dbGetQuery(westat_con, MEC_date_query)

MEC_date_unique <- MEC_date %>% 
  group_by(SP_ID)%>%
  summarise(exam_date=max(SP_Checkin_Time, na.rm = T))

MEC_completes1 <- sqldf(
"SELECT A.* ,  B.*
FROM MEC_completes as A 
LEFT JOIN MEC_date_unique as B 
ON A.spid =B.SP_ID")

freq(MEC_completes$MEC_examined)
class(MEC_completes1$exam_date)
MEC_completes1$exam_date1 <-as.Date(MEC_completes1$exam_date)
class(MEC_completes1$exam_date1)

MEC_completes1 <- subset(MEC_completes1,MEC_examined==1 )

Completed_MECexam_forcast <- MEC_completes1 %>% 
    group_by(standid_p,date_MEC=date(MEC_completes1$exam_date1))%>%
    summarise(N_aggragated_obs_MEC = n(),
              number_of_complete_MECexams =sum(MEC_examined,na.rm=T)) %>%
    mutate (cumulative_sum_complete_MECexams=(cumsum(number_of_complete_MECexams)))

Completed_MECexam_forcast$associated_stand <- with(
    Completed_MECexam_forcast, 
    ifelse(
        standid_p %in% c(428, 345, 407, 331),428,
        ifelse(
            standid_p %in% c(429, 369, 365, 304),429,
            ifelse(
                standid_p %in% c(430, 284, 333, 299),430,
                ifelse(
                    standid_p %in% c(431, 412, 394, 343),431,
                    ifelse(
                        standid_p %in% c(432, 378, 396, 394),432,
                        ifelse(
                            standid_p %in% c(433, 369, 371, 366),433,
                            ifelse(
                                standid_p %in% c(434, 398, 355),434,
                                ifelse(
                                    standid_p %in% c(435, 398, 337, 340, 309),435,
                                    ifelse(
                                        standid_p %in% c(436, 384, 380, 319),436,
                                        ifelse(
                                            standid_p %in% c(437, 386, 387, 319),437,
                                            ifelse(
                                                standid_p %in% c(438, 353, 291, 384),438,
                                                ifelse(
                                                    standid_p %in% c(439, 360, 389, 312),439,
                                                    ifelse(
                                                        standid_p %in% c(440, 350, 287, 379),440,
                                                        ifelse(
                                                            standid_p %in% c(441, 403, 376, 329),441,
                                                            ifelse(
                                                                standid_p %in% c(442, 403, 388, 297),442,
                                                                ifelse(
                                                                    standid_p %in% c(695, 372, 374, 407),695,0
        )))))))))))))))))

Completed_spint_forcast$associated_stand <- with(
    Completed_spint_forcast, 
    ifelse(
        standid %in% c(428, 345, 407, 331),428,
        ifelse(
            standid %in% c(429, 369, 365, 304),429,
            ifelse(
                standid %in% c(430, 284, 333, 299),430,
                ifelse(
                    standid %in% c(431, 412, 394, 343),431,
                    ifelse(
                        standid %in% c(432, 378, 396, 394),432,
                        ifelse(
                            standid %in% c(433, 369, 371, 366),433,
                            ifelse(
                                standid %in% c(434, 398, 355),434,
                                ifelse(
                                    standid %in% c(435, 398, 337, 340, 309),435,
                                    ifelse(
                                        standid %in% c(436, 384, 380, 319),436,
                                        ifelse(
                                            standid %in% c(437, 386, 387, 319),437,
                                            ifelse(
                                                standid %in% c(438, 353, 291, 384),438,
                                                ifelse(
                                                    standid %in% c(439, 360, 389, 312),439,
                                                    ifelse(
                                                        standid %in% c(440, 350, 287, 379),440,
                                                        ifelse(
                                                            standid %in% c(441, 403, 376, 329),441,
                                                            ifelse(
                                                                standid %in% c(442, 403, 388, 297),442,
                                                                ifelse(
                                                                    standid %in% c(695, 372, 374, 407),695,0
                                                                )))))))))))))))))

class(Completed_Screening_forcast$date_screener)

Completed_spint_forcast <- Completed_spint_forcast %>% drop_na(date_SPint)

Completed_spint_forcast1 <-  Completed_spint_forcast %>%
    mutate(date_SPint) %>%
    complete(date_SPint = seq.Date(min(date_SPint), max(date_SPint), by="day")) %>%
    group_by(standid) %>%
    fill(N_aggragated_obs_SPint, number_of_complete_sp, cumulative_sum_complete_sp, associated_stand)


  
Completed_spint_forcast1 <-Completed_spint_forcast1 %>%
  group_by(standid) %>%
  mutate(day= row_number()-1)

Completed_spint_forcast1$week <- week(Completed_spint_forcast1$date_SPint)

Completed_spint_forcast1 <- Completed_spint_forcast1 %>% 
  group_by(standid) %>%
  mutate(week_index = (year(date_SPint) - year(min(date_SPint)))*52 + 
           week(date_SPint) - week(min(date_SPint)))

Completed_spint_forcast1 <- Completed_spint_forcast1 %>% 
  group_by(standid) %>%
  mutate(month_index = (year(date_SPint) - year(min(date_SPint)))*12 + 
           month(date_SPint) - month(min(date_SPint)))

####

Completed_MECexam_forcast <- Completed_MECexam_forcast %>% drop_na(date_MEC)

Completed_MECexam_forcast1 <-  Completed_MECexam_forcast %>%
  mutate(date_MEC) %>%
  complete(date_MEC = seq.Date(min(date_MEC), max(date_MEC), by="day")) %>%
  group_by(standid_p) %>%
  fill(N_aggragated_obs_MEC, number_of_complete_MECexams, cumulative_sum_complete_MECexams, associated_stand)



Completed_MECexam_forcast1 <-Completed_MECexam_forcast1 %>%
  group_by(standid_p) %>%
  mutate(day= row_number()-1)

Completed_MECexam_forcast1$week <- week(Completed_MECexam_forcast1$date_MEC)

Completed_MECexam_forcast1 <- Completed_MECexam_forcast1 %>% 
  group_by(standid_p) %>%
  mutate(week_index = (year(date_MEC) - year(min(date_MEC)))*52 + 
           week(date_MEC) - week(min(date_MEC)))

Completed_MECexam_forcast1 <- Completed_MECexam_forcast1 %>% 
  group_by(standid_p) %>%
  mutate(month_index = (year(date_MEC) - year(min(date_MEC)))*12 + 
           month(date_MEC) - month(min(date_MEC)))

stand_demensions <- read.csv(file = "L:/2021/DHANES dashboard/V0.5/Flat files/stand_demensions.csv")

stand_demensions_dup <- subset(stand_demensions, standid %in% c(319,369,384,394, 398,403,407))

stand_demensions_dup$standid <- with(stand_demensions_dup,
                                  ifelse(
                                    standid==319,'319_2',
                                    ifelse(
                                      standid==369,'369_2',
                                      ifelse(
                                        standid==398, '398_2',
                                        ifelse(
                                          standid==403, '403_2',
                                          ifelse(
                                            standid==384, '384_2',
                                            ifelse(
                                              standid==394, '394_2',
                                              ifelse(
                                                standid==407, '407_2',0
                                              )
                                            )
                                          )
                                        )
                                      )
                                    )
                                  )
)

stand_demensions1 <- rbind(stand_demensions,stand_demensions_dup)
stand_demensions1$standid <-as.character(stand_demensions1$standid)

fwrite(Completed_MECexam_forcast1, file= "C:/Users/qsj2/OneDrive - CDC/MY WORK/Projects/2021/Leading/refusal rates(Ryne)/Analysis/exported data/Completed_MECexam_forcast.csv")
fwrite(Completed_spint_forcast1, file= "C:/Users/qsj2/OneDrive - CDC/MY WORK/Projects/2021/Leading/refusal rates(Ryne)/Analysis/exported data/Completed_spint_forcast.csv")
fwrite(Completed_Screening_forcast1, file= "C:/Users/qsj2/OneDrive - CDC/MY WORK/Projects/2021/Leading/refusal rates(Ryne)/Analysis/exported data/Completed_Screening_forcast.csv")
fwrite(stand_demensions1, file= "C:/Users/qsj2/OneDrive - CDC/MY WORK/Projects/2021/Leading/refusal rates(Ryne)/Analysis/exported data/stand_demensions.csv")


    
