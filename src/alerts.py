import subprocess
import ast
from collections import defaultdict


'''This Module 
        1. Finds the output of hadoop job -list
	2. Processes the output of hadoop job -status <job_id> where job_id is found form step 1
	3. stores the job info obtained in Step 2 in a variable for Alerting Mechnaisms to be initiated

	Alert Mechanism
	1. If no. of mappers > x ( Set in the conf file threshold.conf) '''




''' TODO: Add Alerting Mechanism after the Alarm, which emails to the users/admins'''

class Process(object):
    def __init__(self, dummy='dummy'):
        self.dummy=dummy
	self.current_jobs=0
	self.processed_jobs={}
	self.job_info=defaultdict(dict)
	self.altr=Alert()

    def getJobs(self):
	p = subprocess.Popen(["hadoop", "job","-list"], stdout=subprocess.PIPE)
	out, err = p.communicate()
	self.parseJobs(out)


    def parseJobs(self,job_info):
	job_info_split=job_info.split('\n')
	for line in job_info_split:
		if not line.strip() or 'JobId' in line:
			continue	
		if 'jobs currently running' in line:
			self.current_jobs=int(line[:line.find(' ')].strip())
			continue
		line_split=line.strip().split('\t')
		job_id=line_split[0]
		user_name=line_split[3]
		self.checkIfProcessed(job_id,user_name)
				

    def checkIfProcessed(self,job_id,user_name):
	if job_id not in self.processed_jobs.keys():
		print job_id,'is being processed'
		result=self.Process(job_id,user_name)
		if result is True:
			self.processed_jobs[job_id]=user_name
		self.writeProcessedJobs(self.processed_jobs)

    def Process(self,job_id,user_name):
	p = subprocess.Popen(["hadoop", "job","-status","%s" %(job_id)], stdout=subprocess.PIPE)
        job_info, err = p.communicate()	
	
	job_line_split=job_info.split('\n')
	try:
		for job_item in job_line_split:
			if '=' in job_item:
				job_item_key_value=job_item.strip().split('=')
				key=job_item_key_value[0]
				val=job_item_key_value[1]
				self.job_info[job_id][key]=val
		self.altr.mapperAlert(self.job_info,job_id,user_name)
		return True
	except Exception:
		print "Job finished before analysing"
		return True
	

    def writeProcessedJobs(self,processed_jobs):
	f=open('../conf/processed_jobs','w')
	f.write(str(processed_jobs))
	f.close()	
	
    def readProcessedJobs(self):
	f=open('../conf/processed_jobs','r')
	processed_jobs=f.readline()
	f.close()
	try:
		self.processed_jobs=ast.literal_eval(processed_jobs)	
	except Exception:
		return


    def printVariables(self):
	print self.current_jobs
	print self.processed_jobs
	print self.job_info
	




class Alert(object):
    def __init_(self,dummy='dummy'):
	self.dummy='dummy'

    def mapperAlert(self,job_info,job_id,user_name):
	threshold={}
	f=open('../conf/threshold.conf','r')
	for line in f:
		key,val=line.strip().split(':')
		threshold[key]=val
	f.close()	
	print job_id,user_name,job_info[job_id]['Launched map tasks']
	print threshold['mapper']
	try:
		if int(job_info[job_id]['Launched map tasks']) > int(threshold['mapper']):
			print "Alarm Raised"
	except Exception:
		print " Check the format of configuration"



