library(shiny)
library(lubridate)

name_simplify <- function (colnm){
  colnm = strsplit(x = colnm, split = "_", fixed = T)
  for( i in 1:length(colnm)){
    colnm[[i]][which(nchar(colnm[[i]])>4)] = gsub("(?<!\b)[AaIiUuEeOo]","",colnm[[i]][which(nchar(colnm[[i]])>4)], perl = T)
  }
  colnm = sapply(colnm, function(x){gsub("of_","",paste(x, collapse = "_"))})
  colnm
}


shinyServer(function(input, output) {
  
  filedata <- reactive({
    colnme <- input$colnme
    tablenm <- input$tablenm
    tablenm_target <- input$table_target
    add_where <- input$add_where
    if (colnme=="" && tablenm=="" && tablenm_target=="") {
      # User has not uploaded a file yet
      return(NULL)
    }
    else if(tablenm!="" && colnme==""){
      table_name = tablenm
    }
    else if(tablenm=="" && colnme!="" && tablenm_target==""){
      inputcsv <- toupper(trimws(unlist(strsplit(colnme, split = ","))))
      table_name = "[INSERT TABLE NAME]"
      table_target = "[INSERT TABLE TARGET NAME]"
      
    }
    else if(add_where=="" && tablenm!="" && tablenm_target!="" && colnme!=""){
      inputcsv <- toupper(trimws(unlist(strsplit(colnme, split = ","))))
      table_name = toupper(tablenm)
      table_target = toupper(tablenm_target)
      add_where = ""
    }
    else {
      inputcsv <- toupper(trimws(unlist(strsplit(colnme, split = ","))))
      table_name = toupper(tablenm)
      table_target = toupper(tablenm_target)
      add_where = toupper(paste0(" where ", add_where))
    }
    temp = paste("
   SELECT 
   column AS VARIABLE,
   COUNT(score) AS N,
   AVG(score) AS MEAN,
   STDDEV_SAMP(score) AS STDD,
   VAR_SAMP(score) AS VAR,
   MIN(score) AS MINS,
   MAX(score) AS MAXS,
   percentile_cont(0.1) within group (order by score) AS Q1,
   percentile_cont(0.2) within group (order by score) AS Q2,
   percentile_cont(0.3) within group (order by score) AS Q3,
   percentile_cont(0.4) within group (order by score) AS Q4,
   percentile_cont(0.5) within group (order by score) AS Q5,
   percentile_cont(0.6) within group (order by score) AS Q6,
   percentile_cont(0.7) within group (order by score) AS Q7,
   percentile_cont(0.8) within group (order by score) AS Q8,
   percentile_cont(0.9) within group (order by score) AS Q9
   FROM", table_name)
    
    temp2 = c()
    for (i in 1:length(inputcsv)){
      temp2[i] = gsub(pattern = "column", replacement = paste0('\'',inputcsv[i],'\''),  x = temp)
      temp2[i] = gsub(pattern = "score", replacement = inputcsv[i], x = temp2[i])
    }
    
    temp2[1] = paste0('CREATE MULTISET TABLE ', table_target, ' AS(', temp2[1], add_where, ') WITH DATA;\n')
    
    if(length(inputcsv)>1){
      for(i in 2:length(inputcsv)){
        temp2[i] = paste0('INSERT INTO ', table_target, temp2[i], add_where, ';\n')
      }
    }
    
    paste(temp2, collapse = "")
  })
  
  
  feature_eng <- reactive({
    col_name <- input$col_name
    col_name_date <- input$col_name_date
    prim_key <- input$prim_key
    period_time <- input$period_time
    monthly_tname <- input$monthly_tname
    target_table <- input$target_table
    
    if (col_name=="" && col_name_date=="" && prim_key=="" && period_time=="" && monthly_tname=="" && target_table=="") {
      # User has not uploaded a file yet
      return(NULL)
    }
    else if(period_time!="" && col_name==""){
      period_time
    }
    else if(period_time=="" && col_name!="" && monthly_tname=="" && target_table==""){
      col_name <- toupper(trimws(unlist(strsplit(col_name, split = ","))))
      period_time = "[INSERT COLUMN PERIOD/TIME]"
      monthly_tname = "[INSERT MONTHLY TABLE NAME]"
      target_table = "[INSERT TARGET TABLE NAME]"
    }
    else if(prim_key=="" && period_time=="" && col_name_date!="" && monthly_tname=="" && target_table==""){
      col_name_date <- toupper(trimws(unlist(strsplit(col_name_date, split = ","))))
      period_time = "[INSERT COLUMN PERIOD/TIME]"
      monthly_tname = "[INSERT MONTHLY TABLE NAME]"
      target_table = "[INSERT TARGET TABLE NAME]"
      prim_key = "[INSERT PRIMARY KEY]"
    }
    else if(prim_key!="" && period_time!="" && monthly_tname!="" && col_name!="" && col_name_date =="" && target_table!=""){
      col_name <- toupper(trimws(unlist(strsplit(col_name, split = ","))))
      col_name_date <- ""
      period_time = toupper(period_time)
      monthly_tname = toupper(monthly_tname)
      target_table = toupper(target_table)
    }
    else if(prim_key=="" && period_time!="" && monthly_tname!="" && col_name!="" && col_name_date!="" && target_table!=""){
      col_name <- toupper(trimws(unlist(strsplit(col_name, split = ","))))
      col_name_date <- toupper(trimws(unlist(strsplit(col_name_date, split = ","))))
      period_time = toupper(period_time)
      monthly_tname = toupper(monthly_tname)
      prim_key = "[INSERT PRIMARY KEY]"
      target_table = toupper(target_table)
    }
    else {
      col_name <- toupper(trimws(unlist(strsplit(col_name, split = ","))))
      col_name_date <- toupper(trimws(unlist(strsplit(col_name_date, split = ","))))
      period_time = toupper(period_time)
      monthly_tname = toupper(monthly_tname)
      prim_key = toupper(prim_key)
      target_table = toupper(target_table)
    }
    
    num_col_ori <- col_name
    date_col_ori <- col_name_date
  
    num_col <- name_simplify(num_col_ori)
    date_col <- name_simplify(date_col_ori)
    
    #--------------------------------------------------------------------------------
    #             latest month
    #--------------------------------------------------------------------------------
    num_col_0 = paste0(num_col,"_0")
    
    date_col_0 = paste0(date_col,"_0")
    
    date_col_last = paste0("ADD_MONTHS(( ", date_col_ori, " - EXTRACT(DAY FROM ", date_col_ori, ") + 1),1)-1 - ", date_col_ori, " as ", date_col_0, collapse = ", ")
    
    date_col_w0 = paste0("floor(extract( day from ", date_col_ori," )/7) as ", date_col, "_w0", collapse = ", ")
    date_col_w0_ = paste0(date_col, "_w0")
    
    date_col_dow0 = paste0("case when td_day_of_week(", date_col_ori, ") in (1,6) then 'workdays' else 'weekend' end as ", date_col, "_dow0", collapse = ", ")
    date_col_dow0_ = paste0(date_col, "_dow0")
    
    if(col_name_date!=""){
      latest_sql = paste(paste0("
create multiset table ", target_table," as(
with z as(
  select ", prim_key, ","), paste(paste0(num_col_ori, " as ", num_col_0, collapse=", "), 
    date_col_last, date_col_w0, date_col_dow0, sep = ", "),"
  from", monthly_tname,"
  where ", period_time,"= (select max(", period_time,") from", monthly_tname,")
),")} else{
      latest_sql = paste(paste0("
create multiset table ", target_table," as(
with z as(
  select ", prim_key, ","), paste(paste0(num_col_ori, " as ", num_col_0, collapse=", "), sep = ", "),"
  from", monthly_tname,"
  where ", period_time,"= (select max(", period_time,") from", monthly_tname,")
),")}

    
    #--------------------------------------------------------------------------------
    #             latest 3 months
    #--------------------------------------------------------------------------------
    #average
    num_col_13 = paste0("avg(", num_col_ori, ") as ", num_col, "_13", collapse = ", ")
    num_col_13_ = paste0(num_col, "_13")
    #min
    num_col_min13 = paste0("min(", num_col_ori, ") as ", num_col, "_min_13", collapse = ", ")
    num_col_min13_ = paste0(num_col, "_min_13", collapse = ", ")
    #max
    num_col_max13 = paste0("max(", num_col_ori, ") as ", num_col, "_max_13", collapse = ", ")
    num_col_max13_ = paste0(num_col, "_max_13", collapse = ", ")
    #std dev
    num_col_sd13 = paste0("stddev_samp(", num_col_ori, ") as ", num_col, "_sd_13", collapse = ", ")
    num_col_sd13_ = paste0(num_col, "_sd_13", collapse = ", ")
    #range
    num_col_range13 = paste0("max(", num_col_ori,")-min(", num_col_ori, ") as ", num_col, "_range_13", collapse = ", ")
    num_col_range13_ = paste0(num_col, "_range_13", collapse = ", ")
    #probability of growth
    num_col_p13 = paste0("case when ", num_col_0, " is null then 0 else cast(",num_col_0," as float)/nullifzero(",num_col_13_,") end as p_", num_col_0, "_13", collapse = ", ")
    
    
    latest3_sql = paste(paste0("
y as(
  select ", prim_key, ","), paste(num_col_13,num_col_min13,num_col_max13,num_col_sd13,num_col_range13, sep=", "),"
  from", monthly_tname,"
  where ", period_time,"in ( SELECT", period_time,"FROM (select", period_time,", ROW_NUMBER() OVER (ORDER BY", period_time,"DESC ) RN from", monthly_tname,"GROUP BY 1)A WHERE RN<=3)
  group by 1
),")


    #--------------------------------------------------------------------------------
    #             All months
    #--------------------------------------------------------------------------------
    #average
    num_col_All = paste0("avg(", num_col_ori, ") as ", num_col, "_All", collapse = ", ")
    num_col_All_ = paste0(num_col, "_All")
    #min
    num_col_minAll = paste0("min(", num_col_ori, ") as ", num_col, "_min_All", collapse = ", ")
    num_col_minAll_ = paste0(num_col, "_min_All", collapse = ", ")
    #max
    num_col_maxAll = paste0("max(", num_col_ori, ") as ", num_col, "_max_All", collapse = ", ")
    num_col_maxAll_ = paste0(num_col, "_max_All", collapse = ", ")
    #std dev
    num_col_sdAll = paste0("stddev_samp(", num_col_ori, ") as ", num_col, "_sd_All", collapse = ", ")
    num_col_sdAll_ = paste0(num_col, "_sd_All", collapse = ", ")
    #range
    num_col_rangeAll = paste0("max(", num_col_ori,")-min(", num_col_ori, ") as ", num_col, "_range_All", collapse = ", ")
    num_col_rangeAll_ = paste0(num_col, "_range_All", collapse = ", ")
    #probability of growth
    num_col_pAll = paste0("case when ", num_col_0, " is null then 0 else cast(",num_col_0," as float)/nullifzero(",num_col_All_,") end as p_", num_col_0, "_All", collapse = ", ")
    
    
    date_col_w1 = paste0("sum(case when floor(extract( day from ", date_col_ori, ")/7)=1 then 1 else 0 end) as ", date_col, "_w1", collapse = ", ")
    date_col_w1_ = paste0(date_col, "_w1")
    date_col_w2 = paste0("sum(case when floor(extract( day from ", date_col_ori, ")/7)=2 then 1 else 0 end) as ", date_col, "_w2", collapse = ", ") 
    date_col_w2_ = paste0(date_col, "_w2")
    date_col_w3 = paste0("sum(case when floor(extract( day from ", date_col_ori, ")/7)=3 then 1 else 0 end) as ", date_col, "_w3", collapse = ", ") 
    date_col_w3_ = paste0(date_col, "_w3")
    date_col_w4 = paste0("sum(case when floor(extract( day from ", date_col_ori, ")/7)=4 then 1 else 0 end) as ", date_col, "_w4", collapse = ", ")
    date_col_w4_ = paste0(date_col, "_w4")
    
    date_col_pw1 = paste("case when ", date_col_w1_, " is null then 0 else cast(",date_col_w1_, "as float)/nullifzero(",date_col_w1_,"+",date_col_w2_,"+",date_col_w3_,"+",date_col_w4_,") end as", paste0("p_",date_col_w1_), collapse = ", ")
    date_col_pw2 = paste("case when ", date_col_w2_, " is null then 0 else cast(",date_col_w2_, "as float)/nullifzero(",date_col_w1_,"+",date_col_w2_,"+",date_col_w3_,"+",date_col_w4_,") end as", paste0("p_",date_col_w2_), collapse = ", ")
    date_col_pw3 = paste("case when ", date_col_w3_, " is null then 0 else cast(",date_col_w3_, "as float)/nullifzero(",date_col_w1_,"+",date_col_w2_,"+",date_col_w3_,"+",date_col_w4_,") end as", paste0("p_",date_col_w3_), collapse = ", ")
    date_col_pw4 = paste("case when ", date_col_w4_, " is null then 0 else cast(",date_col_w4_, "as float)/nullifzero(",date_col_w1_,"+",date_col_w2_,"+",date_col_w3_,"+",date_col_w4_,") end as", paste0("p_",date_col_w4_), collapse = ", ")
    
    date_col_wknd = paste0("sum(case when td_day_of_week(", date_col_ori, ") in (1,6) then 1 else 0 end) ", date_col, "_wknd", collapse = ", ")
    date_col_wknd_ = paste0(date_col, "_wknd")
    date_col_wrk = paste0("sum(case when td_day_of_week(", date_col_ori, ") not in (1,6) then 1 else 0 end) ", date_col, "_wrk", collapse = ", ")
    date_col_wrk_ = paste0(date_col, "_wrk")
    
    date_col_pwknd = paste0("cast(", date_col_wknd_," as float)/nullifzero(",date_col_wknd_,"+",date_col_wrk_,") as p_",date_col_wknd_)
    date_col_pwrk = paste0("cast(", date_col_wrk_," as float)/nullifzero(",date_col_wknd_,"+",date_col_wrk_,") as p_",date_col_wrk_)
    
    if(col_name_date!=""){
      all_sql = paste(paste0("
x as(
  select ", prim_key, ","), paste(num_col_All,num_col_minAll,num_col_maxAll,
                                 num_col_sdAll,num_col_rangeAll, date_col_w1, date_col_w2, date_col_w3, date_col_w4, 
                                 date_col_wknd, date_col_wrk, sep=", "),"
  from", monthly_tname,"
  group by 1
)")} else{
      all_sql = paste(paste0("
x as(
  select ", prim_key, ","), paste(num_col_All,num_col_minAll,num_col_maxAll,
                                         num_col_sdAll,num_col_rangeAll, sep=", "),"
  from", monthly_tname,"
  group by 1
)")}
    if(col_name_date!=""){
      combn_sql = paste0(
        latest_sql, latest3_sql, all_sql,"
select x.", prim_key, ", ", paste(paste0(c(num_col_0, num_col_13_, date_col_0, date_col_w0_, date_col_dow0_), collapse=","), 
                                  num_col_min13_, num_col_max13_, num_col_sd13_, num_col_range13_, sep=", "), ", ",
paste(paste0(c(num_col_All_,date_col_w1_, date_col_w2_, date_col_w3_, date_col_w4_, date_col_wknd_, date_col_wrk_), collapse = ","), num_col_minAll_, num_col_maxAll_, 
      num_col_sdAll_, num_col_rangeAll_, sep=", "), ", ",
paste(num_col_p13, num_col_pAll, date_col_pw1, date_col_pw2, date_col_pw3, date_col_pw4, paste0(c(date_col_pwknd, date_col_pwrk), collapse = ","), sep=", "),"
from x 
left join y on y.", prim_key, "= x.", prim_key,"
left join z on z.", prim_key, "= x.", prim_key,"
) with data primary index(", prim_key, ");
")} else{
  combn_sql = paste0(
    latest_sql, latest3_sql, all_sql,"
select x.", prim_key, ", ", paste(paste0(c(num_col_0, num_col_13_), collapse=","), 
                                  num_col_min13_, num_col_max13_, num_col_sd13_, num_col_range13_, sep=", "), ", ",
paste(paste0(c(num_col_All_), collapse = ","), num_col_minAll_, num_col_maxAll_, num_col_sdAll_, num_col_rangeAll_, sep=", "), ", ",
paste(num_col_p13, num_col_pAll, sep=", "),"
from x 
left join y on y.", prim_key, "= x.", prim_key,"
left join z on z.", prim_key, "= x.", prim_key,"
) with data primary index(", prim_key, ");
")}
    
    toupper(combn_sql)
  })
  
  
  
  output$result <- renderText({ filedata() })
  
  output$result_feature <- renderText({ feature_eng() })
  
  # Add clipboard buttons
  output$clip <- renderUI({
    rclipButton("clipbtn", "Copy Results", filedata(), icon("clipboard"))
  })
  
  output$clip_feature <- renderUI({
    rclipButton("clipbtn", "Copy Results", feature_eng(), icon("clipboard"))
  })
})
