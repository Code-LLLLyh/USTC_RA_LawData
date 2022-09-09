# -*- coding: utf-8 -*-
"""
Created on Tue Sep  6 18:29:42 2022

@author: 92853
"""
import PyMySQL
import pandas as pd
from bs4 import BeautifulSoup
from collections import Counter
import xlsxwriter
from openpyxl import load_workbook,Workbook 
import math

class lawDataProcess():
    
    '''
    处理法律文件的数据
    '''
    def __init__(self):
        self.port=3306           #端口代码
        self.host='localhost'        #连接的主机，这里为本地主机
        self.charset='utf8'     #编码形式
        
    def connectMySQL(self,user_num,password_num):
        '''
        连接到MySQL数据库,构建一个游标
        :param user_num str MySQL用户名
        :param password_num str MySQL密码
        :return con connectionModule 与数据库的连接
        :return cur cursorsModule 游标
        '''
        #与MySQL连接
        con=PyMySQL.connect(host=self.host,password=password_num,
                    port=self.port,user=user_num,charset=self.charset)
        #游标，用于在python使用MySQL的交互过程
        cur=con.cursor()
        
        return con,cur
    
    def selectTerm(self,name):
        '''
        由于在MySQL中就要进行筛选因此编写筛选的条件在此
        :param name str 效力的名字
        :return SQL str SQL语句，包含筛选的条件
        '''
        SQL="Select id,Title,IssueDepartment,IssueDate,EffectivenessDic,Category,Keywords,AllText\
            from pkulaw_law\
            where IssueDate Between '1990.01.01' And '2019.12.31'\
            And EffectivenessDic='"+name+"' "
        
        return SQL
    
    def getData(self,cur,name,path):
        '''
        获取全部的法律数据,并整合成DataFrame形式（只适用于数据量较小的情况下，弃用）
        :param cur cursorModule 交互游标
        :param name str 效力名称
        :param path str 数据储存地址
        :return law_Data DataFrame 法律文件数据
        '''
        #流程 show databases（看库）--use **（使用库）-- show tables（看表）-- select...（筛选数据）
        #执行SQL语句，进入需要的库pkulaw
        intoBases='use pkulaw'
        cur.execute(intoBases)
        #执行SQL语句，获取需要的法律文件数据（limit限制获取条数，pkulaw_law是表名）
        getAllData=self.selectTerm(name)
        cur.execute(getAllData)
        #获取数据，回溯上一条命令的数据(fetchone、fetchmany、fetchall)
        lawDataSample=cur.fetchall()
        #整理成为DataFrame格式
        ticker=['id','Title','IssueDepartment','IssueDate',
                'EffectivenessDic','Category','Keywords',
                'AllText']

        lawData=pd.DataFrame(list(lawDataSample),columns=[ticker])
        
        #储存数据
        lawData.to_excel(path,index=None,engine='xlsxwriter')
        
        return lawData
    
    def textProcess(self,lawData):
        '''
        正则化处理文本数据
        :param lawData dataFrame 法律数据(text是HTML格式)
        :return lawData dataFrame 法律数据(text为一段话)
        '''
        agent=lawData[['AllText']].copy();
        for i in range(0,lawData.shape[0]): 
            soup=BeautifulSoup(agent[i],'html.parser') #具有容错功能
            lawData['AllText'][i]=soup.get_text()
        
        return lawData
    
    def effectType(self,cur,path):
        '''
        获取效力全部类型，并生成一个文件
        :param  cur cursorModule 交互游标 
        :param path str 文件储存地址
        '''
        #执行SQL语句，进入需要的库pkulaw
        intoBases='use pkulaw'
        cur.execute(intoBases)
        #执行SQL语句，获取需要的法律文件数据,并只获取效力指标部分
        getAllData='select EffectivenessDic from pkulaw_law'
        cur.execute(getAllData)
        #获取数据，回溯上一条命令的数据(fetchone、fetchmany、fetchall)
        lawDataSample=cur.fetchall()
        #整合成一个DataFrame格式
        lawData=pd.DataFrame(list(lawDataSample),columns=['EffectivenessDic'])
        #获取全部类型并储存
        effectTypeList=lawData['EffectivenessDic'].tolist()
        effectType=dict(Counter(effectTypeList))
        Effi=pd.DataFrame([effectType])
        Effi.to_excel(path)
    
    def bigDataSave(self,cur,name,path,bitSize):
        '''
        逻辑：先把ID等信息储存,再分批获取Text的内容，python能够加载的数据大小较大
        从MySQL读取时占用的内存空间较大，因此分批从MySQL获取数据（大小数据均适用）
        :param cur cursorModule 交互游标
        :param name str 效力的名称
        :param path str 储存的地址
        :param bitSize int 批次
        :return law DataFrame 经过筛选的大量法律数据 
        '''
        
        def getAddtionInfo(name): 
            '''
            由于在MySQL中就要进行筛选因此编写筛选的条件在此,之筛选前边字节少的内容
            :param name str 效力的名字
            :return SQL str SQL语句，包含筛选的条件
            '''
            SQL="Select id,Title,IssueDepartment,IssueDate,EffectivenessDic,Category,Keywords\
                from pkulaw_law\
                where IssueDate Between '1990.01.01' And '2019.12.31'\
                And EffectivenessDic='"+name+"' "
        
            return SQL
            
        #执行SQL语句，进入需要的库pkulaw
        intoBases='use pkulaw'
        cur.execute(intoBases)
        #先获取SQL语句，再执行，这一步获取了除Text以外的其他数据
        getAdditionData=getAddtionInfo(name)
        cur.execute(getAdditionData)
        #获取数据，回溯上一条命令的数据(fetchone、fetchmany、fetchall)
        lawDataSample=cur.fetchall()
        #整理成为DataFrame格式
        ticker=['id','Title','IssueDepartment','IssueDate',
                'EffectivenessDic','Category','Keywords']

        lawData=pd.DataFrame(list(lawDataSample),columns=ticker)        
        
        ##分批获取Text数据
        bit=math.floor(lawData.shape[0]/bitSize)
        
        #先获取最后一部分剩余数据
        #执行SQL语句，进入需要的库pkulaw
        intoBases='use pkulaw'
        cur.execute(intoBases)
        #因为取整，因此50次实际并不能完全取故将剩余的放在一起
        remainNum=lawData.shape[0]-bitSize*bit
        idSingle=lawData['id'][-remainNum:]
        idNum=tuple(idSingle.values)
        #将剩余的读入数据
        textSQL="Select AllText from pkulaw_law where id in "+str(idNum)
        cur.execute(textSQL)
        #获取数据，回溯上一条命令的数据(fetchone、fetchmany、fetchall)
        lawEnd=cur.fetchall()
        #整理成DataFrame格式
        lawEnd=pd.DataFrame(list(lawEnd),columns=['AllText']);
        
        #分50次获取数据
        for j in range(0,bitSize):
            idBit=tuple(lawData['id'][j*bit:(j+1)*bit].values)
            #执行SQL语句，进入需要的库pkulaw
            intoBases='use pkulaw'
            cur.execute(intoBases)
            #执行SQL语句，获取Text数据
            textSQL="Select AllText from pkulaw_law where id in "+str(idBit)
            cur.execute(textSQL)
            #获取数据，回溯上一条命令的数据(fetchone、fetchmany、fetchall)
            lawDataTextBit=cur.fetchall()
            #整理成为DataFrame格式
            law=pd.DataFrame(list(lawDataTextBit),columns=['AllText']);
            #与之前的数据进行拼接
            lawEnd= pd.concat([law, lawEnd], ignore_index=True)
            
        #将两部分数据拼接起来
        law=pd.concat([lawData,lawEnd],axis=1)        
                
        return law 
   
    def  dataSaveBatches(self,law,path,isBatches=False,batchSize=10):
        '''
        分批储存，当数据量大太时容易报错，小数据直接保存即可，大数据分批保存在不同的Excel中
        :param law DataFrame 整合起来的有用信息
        :param path str 储存的地址
        :param isBatches logi 判断变量，默认为关闭状态，即不需要分批处理
        :param batchSize int 批次数量，默认为10次，可以修改（即保存为几个文件）
        '''
        if isBatches==False:    
            file = pd.ExcelWriter(path)
            law.to_excel(file,startcol=0,index=False,engine='xlsxwriter')
            file.save()
        else:
            #分批处理，计算一批处理的数据量
            bit=math.floor(law.shape[0]/batchSize)
            #需要分两次操作，因为取整，最后一批数据的数据量和之前不同
            for i in range(0,batchSize-1):
                #替换路径保存为文件1，2...
                newPath=path.replace('.xlsx',str(i+1)+'.xlsx')
                file = pd.ExcelWriter(newPath)
                law.iloc[i*bit:(i+1)*bit].to_excel(file,startcol=0,index=False,engine='xlsxwriter')
                file.save()
            #处理最后一批数据
            newPath=path.replace('.xlsx',str(batchSize)+'.xlsx')
            file = pd.ExcelWriter(newPath)
            law.iloc[(batchSize-1)*bit:].to_excel(file,startcol=0,index=False,engine='xlsxwriter')
            file.save()    
                
    def exitMySQL(self,cur,con):
        '''
        退出连接的MySQL
        :param con connectionModule 与数据库的连接
        :param cur cursorsModule 游标
        '''
        cur.close()
        con.close()
        
        
if __name__ == '__main__':
    
    #设定登陆账号和密码
    user_num='root'
    password_num='youthforeverLYH0'
    
    ##前述准备
    #引入法律数据处理函数类
    lawDataProcss=lawDataProcess()
    #连接到MySQL
    con,cur=lawDataProcss.connectMySQL(user_num,password_num)
    
    #获取数据并储存
    effectType="地方规范性文件"
    path='E:/2022/Weiliang Zhang Group/Data/'+effectType+'.xlsx'
    lawData=lawDataProcss.bigDataSave(cur,effectType,path,bitSize=300)
    #储存数据
    lawDataProcss.dataSaveBatches(lawData,path,isBatches=True,batchSize=15)
    
    #处理数据（指把Text文件整理成文字
    #lawData=lawDataProcss.textProcess(lawData)
    
    #退出连接
    #lawDataProcss.exitMySQL(cur,con)

    
                
