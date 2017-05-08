source("cust_ts_model1.R")
# rm(list=ls())

#library(RMySQL)
library(dplyr)
#library(data.table)
#library(lubridate)
library(RJDBC)
library(parallel)
library(doParallel)
options(stringsAsFactors = F)

cat('busi_pred_week.r开始:.....\n')
print(Sys.time())
###此处注意是ddtmain库连接串
cat('连接main库:.....\n')
drv<- RJDBC::JDBC('com.mysql.jdbc.Driver', "mysql-connector-java-5.1.6.jar")
m1 <- RJDBC::dbConnect(drv, "jdbc:mysql://DDTMAIN-M.db.sfdc.com.cn:3306/ddtmain", "ddtmain", "xxx")




#connect busi week data 
a = dbGetQuery(m1,'select * from temp_targetsku_week')

a$company_code='58773096-8'



sku.split.week<-function(sku_count,s,level=''){
  sku_count<-sku_count[sku_count$SKU %in% s,]
  a1=dplyr::group_by(sku_count,WEEKNUM,TO_CITY_CODE)
  a1=dplyr::summarise(a1,ORDER_NUM=sum(ORDER_NUM))
  a1=as.data.frame(a1)
  a1$SKU<-level
  return(a1[,c("WEEKNUM","SKU","TO_CITY_CODE","ORDER_NUM")])
}



#####根据货主获取SKU信息
cat('获取SKU信息:.....\n')
sku_extract_week<-function(sku_count){
  
  colnames(sku_count)<-toupper(colnames(sku_count))
  sku_count$ORDER_NUM<-as.integer(sku_count$ORDER_NUM)
  mm=nrow(sku_count)
  ####判断数据情况 
  flag=1
  wk=''
  sku_top=''
  if(mm<10){
    cat(as.character(Sys.time()),company_code,'无数据.\n')
    flag=-1
  }
  else{
    wk=sort(unique(sku_count$WEEKNUM))
    
    flag=1
    # as.integer(format(Sys.Date(),'%Y%U'))-as.integer(max(wk))
    
    if(flag>=2 ){
      cat(as.character(Sys.time()),company_code,'2week内无数据更新，无需更新预测结果.\n')
    }
  }
  
  ####汇总数据
  cat('汇总数据:.....\n') 
  if(flag>0 & flag<60){
    sku_count_last<-sku_count[sku_count$WEEKNUM>as.character(format(Sys.Date()-165,'%Y%U')),]
    cat('统计数据:.....\n')
    
    #### 最近30天SKU销量统计
    s1=sort(tapply(sku_count_last$ORDER_NUM,sku_count_last$SKU,sum),decreasing = T)
    s2=sort(tapply(sku_count_last$ORDER_NUM,sku_count_last$SKU,function(x){length(unique(x))}),decreasing = T)
    
    ####统计Top销量
    s1_df<-data.frame(SKU=names(s1),ORDER_NUM=as.vector(s1))
    s1_df$CUM_NUM=cumsum(s1_df$ORDER_NUM)
    sku_sum=sum(s1_df$ORDER_NUM)
    s1_df$CUM_PER=s1_df$CUM_NUM/sku_sum
    s1_df$TOP_LEVEL=sapply(s1_df$CUM_PER,.sku_level)
    
    #### ALL,30%,50%,80%  
    a1=sku.split.week(sku_count,s=s1_df$SKU,level='ALL')
    a3=sku.split.week(sku_count,s=s1_df$SKU[s1_df$TOP_LEVEL==30],level='30')
    a5=sku.split.week(sku_count,s=s1_df$SKU[s1_df$TOP_LEVEL<80],level='50')
    a8=sku.split.week(sku_count,s=s1_df$SKU[s1_df$TOP_LEVEL<100],level='80')
    
    sku_top<-s1_df[ s1_df$SKU %in% names(s2[s2>5]),c('SKU','TOP_LEVEL','ORDER_NUM') ]
    sku_count<-sku_count[sku_count$SKU %in% sku_top$SKU, ]
    sku_count<-rbind(sku_count,a1,a3,a5,a8)
    
    toptmp=data.frame(SKU=c('ALL','30','50','80'),
                      TOP_LEVEL=c('100','30','50','80'),
                      ORDER_NUM=c(sum(a1$ORDER_NUM),sum(a3$ORDER_NUM),sum(a5$ORDER_NUM),sum(a8$ORDER_NUM)))
    sku_top<-rbind(toptmp,sku_top)
    
    b1=dplyr::group_by(sku_count,WEEKNUM,SKU)
    b1=dplyr::summarise(b1,ORDER_NUM=sum(ORDER_NUM))
    b1<-as.data.frame(b1)
    b1$TO_CITY_CODE<-'ALL'
    sku_count=rbind(sku_count,b1[,c("WEEKNUM","SKU","TO_CITY_CODE","ORDER_NUM" )])
    sku_iter<-unique(sku_count[,c('SKU','TO_CITY_CODE')])
    rm(a1,a3,a5,a8,sku_count_last,b1)
    cat('预测SKU数量:',nrow(sku_top)-4,'\n')
    row.names(sku_top)<-NULL
    row.names(sku_count)<-NULL
    sku_iter<-sku_iter[!is.na(sku_iter$TO_CITY_CODE),]
  }
  out<-list(ori_data=sku_count,sku_top=sku_top,sku_iter=sku_iter,wk=wk,flag=flag)
  rm(sku_count)
  return(out)
}



############################################
cname=c('COMPANY_CODE','SKU','TOP_LEVEL','CITY_CODE','CUR_WEEK','WEEKNUM',
        'R_TICKETS','P_TICKETS','RSQUARE','CREATE_TM')
in_name=c('COMPANY_CODE','SKU','TOP_LEVEL','CITY_CODE','CUR_WEEK','SEQ_TIME',
          'SEQ_X','PRED','RSQUARE','CREATE_TM')


####指定货主
company_code='58773096-8'



a1=a;a1$company_code=NULL;
str(a1)
a1$WEEKNUM=as.character(a1$WEEKNUM)
a1$TO_CITY_CODE=as.character(a1$TO_CITY_CODE)



####判断数据情况
cat('判断数据:.....\n')
sku_tmp=sku_extract_week(a1)

flag=sku_tmp$flag
ori_data=sku_tmp$ori_data
sku_top<-sku_tmp$sku_top
sku_iter<-sku_tmp$sku_iter
wk<-sku_tmp$wk


wknum<-max(wk)
flag=min(1,flag) ## 
#########################################################################

#### 注册cpu个数
start_time=Sys.time()
cpu <- parallel::detectCores()
cpu=ifelse(cpu<3,cpu,cpu-1)
cl <- parallel::makePSOCKcluster(cpu)
doParallel::registerDoParallel(cl)
if(flag<3 & flag>0){
  NN=nrow(sku_iter)
  #### duo
  result=foreach::foreach(i=1:NN,.combine=rbind,.multicombine=T) %dopar% {
    city_code=sku_iter$TO_CITY_CODE[i]
    item=sku_iter$SKU[i]
    toplevel=sku_top$TOP_LEVEL[sku_top$SKU==item]
    
    indx=which(ori_data$TO_CITY_CODE==city_code & ori_data$SKU==item)
    x=ori_data[indx,]
    min_wk=min(x$WEEKNUM)
    x=x[order(x$WEEKNUM),]
    if(nrow(x)>3){
      mess<-paste(as.character(Sys.time()),paste0(i,'/',NN),company_code,item,city_code,nrow(x))
      print(mess)
      out1<-NULL
      out1<-tryCatch(predict.city.week(x,wk=wk[wk>=min_wk]),
                     error = function(e){print(e);return(NULL)})
      if(!is.null(out1)){
        if(out1$is_pred==1){
          pred=out1$seq_data
          pred$TOP_LEVEL=toplevel
          pred$sku=item
          pred$cur_week=wknum
          pred$city_code=city_code
          pred$company_code<-company_code
          colnames(pred)<-toupper(colnames(pred))
          V=pred[,in_name]
          colnames(V)<-cname
          (V)
        }
      }
    }
    
  }
} 
result$WEEKNUM<-as.character(result$WEEKNUM)    
cost_time=Sys.time()-start_time
stopCluster(cl)
cat('cost time:',cost_time,'\n')
# cost time: 8.677669 


cat('注册CPU结束:.....\n')

###################################################
###汇总结果 ###此处注意是ddtmain库连接串
drv<- RJDBC::JDBC('com.mysql.jdbc.Driver', "mysql-connector-java-5.1.6.jar")
m1 <- RJDBC::dbConnect(drv, "jdbc:mysql://DDTMAIN-M.db.sfdc.com.cn:3306/ddtmain", "ddtmain", "xxx")


###城市维表
cat('城市维表:.....\n') 			  
dim_city<-RJDBC::dbGetQuery(m1,"select * from temp_address where city<>0")

result_city=result[result$CITY_CODE!='ALL' & is.na(result$R_TICKETS),
                   c('SKU','TOP_LEVEL','CITY_CODE','P_TICKETS')]
str(result_city)
str(dim_city)
dim_city$province=as.character(dim_city$province)
dim_city$city=as.character(dim_city$city)

city_pro<-dim_city[match(result_city$CITY_CODE,dim_city$city),c('city_name','province_name')]
result_city<-cbind(result_city,city_pro)

#### 统计城市占比
cat('统计城市占比:.....\n')
result_city_rate=as.data.frame(summarise(group_by(result_city,SKU,TOP_LEVEL,city_name),
                                         P_TICKETS_SUM=sum(P_TICKETS)
))

result_city_rate$P_CITY_RATE<-0
MM=nrow(sku_top)
for(j in 1:MM){
  ind=which(result_city_rate$SKU==sku_top$SKU[j])
  if(length(ind)>0){
    tmp=result_city_rate[ind,'P_TICKETS_SUM']
    result_city_rate$P_CITY_RATE[ind]=round(100*tmp/(sum(tmp)+0.00001),2)
  }
}
tag=RJDBC::dbGetQuery(m1,"select * from tag")



t_ddt_rpt_waybill_predict_city<-data.frame(ODS_DTE=rep(Sys.Date()-1,nrow(result_city_rate)),
                                           CUSTOMER_CODE=company_code, DATA_RANGE=tag$n)
t_ddt_rpt_waybill_predict_city<-cbind(t_ddt_rpt_waybill_predict_city,result_city_rate[,c(1,3,4,5)])
t_ddt_rpt_waybill_predict_city_b<-t_ddt_rpt_waybill_predict_city[,c(1,2,4,3,5,6,7)]
colnames(t_ddt_rpt_waybill_predict_city_b)<-c("ODS_DTE","CUSTOMER_CODE","SKU_TYPE","DATA_RANGE","CITY", "WAYBILL_PRE","WAYBILL_RATIO")



###统计省份占比
cat('统计省份占比:.....\n')
result_province=as.data.frame(summarise(group_by(result_city,SKU,TOP_LEVEL,province_name),
                                        P_TICKETS_SUM=sum(P_TICKETS)))

result_province$P_RATE<-0
MM=nrow(sku_top)
for(j in 1:MM){
  ind=which(result_province$SKU==sku_top$SKU[j])
  if(length(ind)>0){
    tmp=result_province[ind,'P_TICKETS_SUM']
    result_province$P_RATE[ind]=round(100*tmp/(sum(tmp)+0.00001),2)
  }
}

t_ddt_rpt_waybill_predict_area<-data.frame(ODS_DTE=rep(Sys.Date()-1,nrow(result_province)),
                                           CUSTOMER_CODE=company_code, DATA_RANGE=tag$n)
t_ddt_rpt_waybill_predict_area<-cbind(t_ddt_rpt_waybill_predict_area,result_province[,c(1,3,4,5)])
t_ddt_rpt_waybill_predict_area_b<-t_ddt_rpt_waybill_predict_area[,c(1,2,4,3,5,6,7)]
colnames(t_ddt_rpt_waybill_predict_area_b)<-c("ODS_DTE","CUSTOMER_CODE","SKU_TYPE","DATA_RANGE","PROVINCE", "WAYBILL_PRE","WAYBILL_RATIO")




###件量TOP10款式
cat('件量TOP10款式:.....\n')
### 件量TOP10款式
result_sku=result[result$CITY_CODE=='ALL' & is.na(result$R_TICKETS),
                  c('SKU','TOP_LEVEL','P_TICKETS')]

# result_sku_all=result_sku[result_sku$SKU=='ALL','P_TICKETS']
result_sku_all=nrow(a1)
result_sku_top=result_sku[!result_sku$SKU %in% c('ALL','30','50','80'),]
result_sku_top$P_RATE<-round(100*result_sku_top$P_TICKETS/result_sku_all,2)
result_sku_top<-result_sku_top[order(result_sku_top$P_RATE,decreasing = T),]

###更改
protoct3= dbGetQuery(m1,'select * from temp_sku_ct3')

result_sku_top1 = inner_join(result_sku_top,protoct3,by=c("SKU"="product_number"))

result_sku_top2=as.data.frame(summarise(group_by(result_sku_top1,category_three_code),
                                        nrows=round(sum(P_TICKETS))))



result_sku_top3=inner_join(result_sku_top1,result_sku_top2,by=c("category_three_code"="category_three_code"))


result_sku_top3$P_RATE1<-round(100*result_sku_top3$P_TICKETS/result_sku_top3$nrows,2)


t_ddt_rpt_waybill_predict_sku1<-data.frame(ODS_DTE=rep(Sys.Date()-1,nrow(result_sku_top3)),
                                           CUSTOMER_CODE=company_code, DATA_RANGE=tag$n)
t_ddt_rpt_waybill_predict_sku1<-cbind(t_ddt_rpt_waybill_predict_sku1,result_sku_top3)
t_ddt_rpt_waybill_predict_sku_b<-t_ddt_rpt_waybill_predict_sku1[,c(1,2,11,3,4,6,13)]
colnames(t_ddt_rpt_waybill_predict_sku_b)<-c("ODS_DTE","CUSTOMER_CODE","SKU_TYPE","DATA_RANGE","SKU", "WAYBILL_PRE","WAYBILL_RATIO")






#####件量趋势
cat('件量趋势:.....\n')
result1=result[result$CITY_CODE=='ALL',c('COMPANY_CODE','SKU','WEEKNUM','P_TICKETS','R_TICKETS')]
t_ddt_rpt_waybill_predict_b<-data.frame(ODS_DTE=as.character(Sys.Date()-1),
                                        DATA_RANGE=tag$n,result1)
t_ddt_rpt_waybill_predict_b<-t_ddt_rpt_waybill_predict_b[,c(1,3,4,2,5,6,7)]
colnames(t_ddt_rpt_waybill_predict_b)<-c("ODS_DTE","CUSTOMER_CODE","SKU_TYPE","DATA_RANGE","WAYBILL_DATE", "WAYBILL_PRE","WAYBILL_ACT")





####  MYSQL 写入
cat('Mysql写入:.....\n')
RJDBC::dbDisconnect(m1) ## 

#此处注意是ddtdata库连接串
cat('连接data库:.....\n')
drv<- RJDBC::JDBC('com.mysql.jdbc.Driver', "mysql-connector-java-5.1.6.jar")
m1 <- RJDBC::dbConnect(drv, "jdbc:mysql://DDTDATA-M.db.sfdc.com.cn:3306/ddtdata", "ddtdata", "xxx")




#### 预测明细写入
cat('预测明细写入:.....\n')
dbSendUpdate(m1,'truncate table cust_pred_week_b')	
RJDBC::dbWriteTable(m1, "cust_pred_week_b",result,
                    overwrite=TRUE)



#####件量趋势写入
cat('件量趋势写入:.....\n')		   		   
dbSendUpdate(m1,'truncate table t_ddt_rpt_waybill_predict_bw')										
dbWriteTable(m1, "t_ddt_rpt_waybill_predict_bw",t_ddt_rpt_waybill_predict_b,
             overwrite=TRUE)

dbSendUpdate(m1,'INSERT INTO t_ddt_rpt_waybill_predict_b (ODS_DTE, CUSTOMER_CODE, SKU_TYPE, DATA_RANGE, WAYBILL_DATE, WAYBILL_PRE, WAYBILL_ACT) select ODS_DTE, CUSTOMER_CODE, SKU_TYPE, DATA_RANGE, WAYBILL_DATE, WAYBILL_PRE, WAYBILL_ACT from t_ddt_rpt_waybill_predict_bw;')



#####城市占比写入	   
cat('城市占比写入:.....\n')
dbSendUpdate(m1,'truncate table t_ddt_rpt_waybill_predict_city_bw')										
dbWriteTable(m1, "t_ddt_rpt_waybill_predict_city_bw",t_ddt_rpt_waybill_predict_city_b,
             overwrite=TRUE)

dbSendUpdate(m1,'INSERT INTO t_ddt_rpt_waybill_predict_city_b (ODS_DTE, CUSTOMER_CODE, SKU_TYPE, DATA_RANGE, CITY, WAYBILL_PRE, WAYBILL_RATIO) select ODS_DTE, CUSTOMER_CODE, SKU_TYPE, DATA_RANGE,  CITY, WAYBILL_PRE, WAYBILL_RATIO from t_ddt_rpt_waybill_predict_city_bw;')


#####省份占比写入
cat('省份占比写入:.....\n')
dbSendUpdate(m1,'truncate table t_ddt_rpt_waybill_predict_area_bw')										
dbWriteTable(m1, "t_ddt_rpt_waybill_predict_area_bw",t_ddt_rpt_waybill_predict_area_b,
             overwrite=TRUE)

dbSendUpdate(m1,'INSERT INTO t_ddt_rpt_waybill_predict_area_b (ODS_DTE, CUSTOMER_CODE, SKU_TYPE, DATA_RANGE, PROVINCE, WAYBILL_PRE, WAYBILL_RATIO) select ODS_DTE, CUSTOMER_CODE, SKU_TYPE, DATA_RANGE,  PROVINCE, WAYBILL_PRE, WAYBILL_RATIO from t_ddt_rpt_waybill_predict_area_bw;')



####件量TOP10款式 写入
cat('件量TOP10款式写入:.....\n')
dbSendUpdate(m1,'truncate table t_ddt_rpt_waybill_predict_sku_bw')										
dbWriteTable(m1, "t_ddt_rpt_waybill_predict_sku_bw",t_ddt_rpt_waybill_predict_sku_b,
             overwrite=TRUE)
dbSendUpdate(m1,'drop table t_ddt_rpt_waybill_predict_sku_bw1')
dbSendUpdate(m1,'create table t_ddt_rpt_waybill_predict_sku_bw1 as select * from t_ddt_rpt_waybill_predict_sku_bw group by SKU')

dbSendUpdate(m1,'INSERT INTO t_ddt_rpt_waybill_predict_sku_b (ODS_DTE, CUSTOMER_CODE, SKU_TYPE, DATA_RANGE, SKU, WAYBILL_PRE, WAYBILL_RATIO) select ODS_DTE, CUSTOMER_CODE, SKU_TYPE, DATA_RANGE,  SKU, WAYBILL_PRE, WAYBILL_RATIO from t_ddt_rpt_waybill_predict_sku_bw1;')





RJDBC::dbDisconnect(m1) ## 

print(Sys.time())                     
cat('busi_pred_week.r结束:.....\n')




### 修改部分
####  MYSQL 写入
cat('Mysql写入DDTYC:.....\n')


#此处注意是ddtyc库连接串
drv<- RJDBC::JDBC('com.mysql.jdbc.Driver', "mysql-connector-java-5.1.6.jar")
m1 <- RJDBC::dbConnect(drv, "jdbc:mysql://ddtmain-ddtyc-m.db.sfdc.com.cn:3306/ddtyc", "ddtyc", "xxx")



#### 预测明细写入
cat('预测明细写入:.....\n')
dbSendUpdate(m1,'truncate table cust_pred_week_b')	
RJDBC::dbWriteTable(m1, "cust_pred_week_b",result,
                    overwrite=TRUE)



#####件量趋势写入
cat('件量趋势写入:.....\n')		   		   
dbSendUpdate(m1,'truncate table t_ddt_rpt_waybill_predict_bw')										
dbWriteTable(m1, "t_ddt_rpt_waybill_predict_bw",t_ddt_rpt_waybill_predict_b,
             overwrite=TRUE)

dbSendUpdate(m1,'INSERT INTO t_ddt_rpt_waybill_predict_b (ODS_DTE, CUSTOMER_CODE, SKU_TYPE, DATA_RANGE, WAYBILL_DATE, WAYBILL_PRE, WAYBILL_ACT) select ODS_DTE, CUSTOMER_CODE, SKU_TYPE, DATA_RANGE, WAYBILL_DATE, WAYBILL_PRE, WAYBILL_ACT from t_ddt_rpt_waybill_predict_bw;')



#####城市占比写入	   
cat('城市占比写入:.....\n')
dbSendUpdate(m1,'truncate table t_ddt_rpt_waybill_predict_city_bw')										
dbWriteTable(m1, "t_ddt_rpt_waybill_predict_city_bw",t_ddt_rpt_waybill_predict_city_b,
             overwrite=TRUE)

dbSendUpdate(m1,'INSERT INTO t_ddt_rpt_waybill_predict_city_b (ODS_DTE, CUSTOMER_CODE, SKU_TYPE, DATA_RANGE, CITY, WAYBILL_PRE, WAYBILL_RATIO) select ODS_DTE, CUSTOMER_CODE, SKU_TYPE, DATA_RANGE,  CITY, WAYBILL_PRE, WAYBILL_RATIO from t_ddt_rpt_waybill_predict_city_bw;')


#####省份占比写入
cat('省份占比写入:.....\n')
dbSendUpdate(m1,'truncate table t_ddt_rpt_waybill_predict_area_bw')										
dbWriteTable(m1, "t_ddt_rpt_waybill_predict_area_bw",t_ddt_rpt_waybill_predict_area_b,
             overwrite=TRUE)

dbSendUpdate(m1,'INSERT INTO t_ddt_rpt_waybill_predict_area_b (ODS_DTE, CUSTOMER_CODE, SKU_TYPE, DATA_RANGE, PROVINCE, WAYBILL_PRE, WAYBILL_RATIO) select ODS_DTE, CUSTOMER_CODE, SKU_TYPE, DATA_RANGE,  PROVINCE, WAYBILL_PRE, WAYBILL_RATIO from t_ddt_rpt_waybill_predict_area_bw;')



####件量TOP10款式 写入
cat('件量TOP10款式写入:.....\n')
dbSendUpdate(m1,'truncate table t_ddt_rpt_waybill_predict_sku_bw')										
dbWriteTable(m1, "t_ddt_rpt_waybill_predict_sku_bw",t_ddt_rpt_waybill_predict_sku_b,
             overwrite=TRUE)
dbSendUpdate(m1,'drop table t_ddt_rpt_waybill_predict_sku_bw1')
dbSendUpdate(m1,'create table t_ddt_rpt_waybill_predict_sku_bw1 as select * from t_ddt_rpt_waybill_predict_sku_bw group by SKU')

dbSendUpdate(m1,'INSERT INTO t_ddt_rpt_waybill_predict_sku_b (ODS_DTE, CUSTOMER_CODE, SKU_TYPE, DATA_RANGE, SKU, WAYBILL_PRE, WAYBILL_RATIO) select ODS_DTE, CUSTOMER_CODE, SKU_TYPE, DATA_RANGE,  SKU, WAYBILL_PRE, WAYBILL_RATIO from t_ddt_rpt_waybill_predict_sku_bw1;')





RJDBC::dbDisconnect(m1) ## 

print(Sys.time())                     
cat('写入DDTYC结束:.....\n')







