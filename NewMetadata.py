
import tableauserverclient as TSC
import os, traceback
import xml.etree.ElementTree as ET
from tableaudocumentapi import Workbook
import shutil
import zipfile
import glob
import pandas as pd

##Variables
data = []
dash=[]
story=[]
worksheetdatasource=[]
workbooks=[]
worksheetstats=[]
connections=[]

##Connection
tableau_auth = TSC.TableauAuth('login', 'password')
server = TSC.Server('https://tableau.server.com')
server.auth.sign_in(tableau_auth)

all_sites, pagination_item = server.sites.get()
sites=pd.DataFrame(columns = ['name', 'content_url', 'w3', 'w4', 'w5'])
sites.append(all_sites)
server.auth.sign_out()
##For Each Site
for site in all_sites:

        tableau_auth = TSC.TableauAuth('login', 'password',site.content_url)
        server.auth.sign_in(tableau_auth)
        all_workbooks = list(TSC.Pager(server.workbooks))
        ##For Each Workbook
        for workbook in all_workbooks:
            if workbook.id=="54fb14ca-7395-47fa-b6db-af568699bd92":
                try:
                    ##Download Workbook
                    workbooks.append([workbook.id,workbook.name,workbook.owner_id,workbook.project_id,workbook.project_name,site.name])
                    file_path = server.workbooks.download(workbook.id,no_extract=True)
                    print("Downloaded the file to {0}".format(file_path))
                    extension=file_path.split(".")[len(file_path.split("."))-1]
                    tree=""
                    ##Manage extenssion
                    if extension == "twb":
                        wb = Workbook(file_path)
                        tree = ET.parse(file_path)
                        os.remove(file_path)
                    else:
                        mzip = os.rename(file_path,"temp.zip" )
                        zip = zipfile.ZipFile("temp.zip")
                        zip=zip.extractall('temp')
                        name=""
                        for f in glob.glob('temp/*.twb'):
                            name=f
                        wb = Workbook(name)
                        tree = ET.parse(name)
                        os.remove("temp.zip")
                        shutil.rmtree("temp")
                        root = tree.getroot()
                        ##Get Data for Dashboards
                        for dashboard in root.findall("./windows/window/[@class='dashboard']"):
                            for viewpoints in dashboard:
                                for viewpoint in viewpoints:
                                    dash.append([workbook.id,workbook.name,dashboard.attrib["name"],viewpoint.attrib["name"]])
                        ##Get Data for Worksheet
                        for worksheet in root.findall("./worksheets/"):
                            for datasource in worksheet.findall("./table/view/datasources/"):
                                if datasource.attrib["name"]!="Parameters": 
                                    if datasource.attrib.has_key('caption'):
                                        worksheetdatasource.append([workbook.id,workbook.name,worksheet.attrib["name"],datasource.attrib['caption']])
                                    else:
                                        worksheetdatasource.append([workbook.id,workbook.name,worksheet.attrib["name"],datasource.attrib['name']])
                        ##Get Data for Datasources                
                        for ds in wb.datasources:
                            if ds.name != 'Parameters':
                                for key, col in ds.fields.iteritems():
                                    for ws in col.worksheets:
                                        data.append([ds.caption,workbook.id,workbook.name,ws, col.name, col.datatype, col.role, col.caption, col.alias, col.calculation, col.description ])
                except Exception:
                    print("Issue with {0}",workbook.name)
                    traceback.print_exc()
                    pass
                try:
                    os.remove("temp.zip")
                    shutil.rmtree("temp")
                except Exception:
                    pass
        server.auth.sign_out()

##Build Dataframe
cols=['Datasource','Workbook LUID','Workbook Name','Worksheet','Column Name','Column Datatype',"Column Role","Column Caption","Column Alias","Column Calculation","Column Description"]
results = pd.DataFrame(data, columns=cols) 
cols=['Workbook LUID','Workbook Name','Dashboard','Worksheet']
dashboardf = pd.DataFrame(dash, columns=cols)
cols=['Workbook LUID','Workbook Name','Worksheet','Datasource']
worksheet_datasource = pd.DataFrame(worksheetdatasource, columns=cols)
cols=['Workbook LUID','workbook Name','Owner ID','Project ID','Project Name','Site']
workbooks = pd.DataFrame(workbooks, columns=cols) 
try:
    os.remove("metadata.csv")
    os.remove("dashboard.csv")
    os.remove("storyboard.csv")
    os.remove("worksheetdatasource.csv")
    os.remove("workbooks.csv")
except OSError:
    pass

results=results.drop_duplicates(subset=['Datasource','Workbook LUID','Worksheet','Column Name'], keep=False)

##Export
results.to_csv("metadata.csv", sep='\t', encoding='utf-8')
dashboardf.to_csv("dashboard.csv", sep='\t', encoding='utf-8')
worksheet_datasource.to_csv("worksheetdatasource.csv", sep='\t', encoding='utf-8')
workbooks.to_csv("workbooks.csv", sep='\t', encoding='utf-8')



