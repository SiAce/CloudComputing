from pyspark import SparkContext, SparkConf
from math import sqrt
import csv

conf = SparkConf().setAppName('HR')
sc = SparkContext(conf=conf)

# read data and split every line of data
lines = sc.textFile('/home/hadoop/workspace/HR_comma_sep.csv')
lineOfData = lines.map(lambda s: s.split(','))

# make lineOfData persistent because it will be used several times
lineOfData.persist()

def point_biserial(x):
# calculate the point-biserial correlation coefficient of variable x and variable "left"
	stdev = lineOfData.map(lambda a: float(a[x])).stdev()
	m1 = lineOfData.filter(lambda a: int(a[9]) == 1).map(lambda a: float(a[x])).mean()
	m0 = lineOfData.filter(lambda a: int(a[9]) == 0).map(lambda a: float(a[x])).mean()
	n1 = lineOfData.filter(lambda a: int(a[9]) == 1).count()
	n0 = lineOfData.filter(lambda a: int(a[9]) == 0).count()
	n = lineOfData.count()
	return (m1 - m0)/stdev*sqrt(1.0*n1*n0/(n*n))

def cramer_v(x):
# calculate the Cramer's V correlation coefficient of variable x and variable "left"
	row = list(set(lineOfData.map(lambda a: a[x]).collect()))
	column = list(set(lineOfData.map(lambda a: a[9]).collect()))
	n = lineOfData.count()
	chi2 = 0
	for i in row:
		for j in column:
			nij = float(lineOfData.filter(lambda a: a[x] == i and a[9] == j).count())
			ni = float(lineOfData.filter(lambda a: a[x] == i).count())
			nj = float(lineOfData.filter(lambda a: a[9] == j).count())
			chi2 += (nij - ni*nj/n)**2 / (ni*nj/n)
	v = sqrt(chi2/n)
	return v

# calculate data about satisfaction level
satisfactionLevel = ['satisfaction_level']
for i in range(5):
        # calculate total amount of people whose satisfaction levels are in (0.2i,0.2(i+1)]
	satisfaction = lineOfData.filter(lambda a: float(a[0])>0.2*i and float(a[0])<=0.2*(i+1))
	# calculate the amount of peole who have left in this range
	peopleLeft = satisfaction.filter(lambda a: int(a[9]) == 1)
	# calculate the percentage
	satisfactionLevel.append(1.0*peopleLeft.count()/satisfaction.count())
satisfactionLevel.append('point_biserial=%f' % point_biserial(0))

# calculate data about last evaluation
lastEvaluation = ['last_evaluation']
for i in range(1,5):
	evaluation = lineOfData.filter(lambda a: float(a[1])>0.2*i and float(a[1])<=0.2*(i+1))
	peopleLeft = evaluation.filter(lambda a: int(a[9]) == 1)
	lastEvaluation.append(1.0*peopleLeft.count()/evaluation.count())
lastEvaluation.append('point_biserial=%f' % point_biserial(1))

numberProject = ['Number_project']
for i in range(2,8):
	project = lineOfData.filter(lambda a: int(a[2]) == i)
	peopleLeft = project.filter(lambda a: int(a[9]) == 1)
	numberProject.append(1.0*peopleLeft.count()/project.count())
numberProject.append('point_biserial=%f' % point_biserial(2))

averageMonthlyHour = ['average_monthly_hour']
for i in range(5):
	monthlyHour = lineOfData.filter(lambda a: int(a[3]) >=96+43*i and int(a[3]) <96+43*(i+1))
	peopleLeft = monthlyHour.filter(lambda a: int(a[9]) == 1)
	averageMonthlyHour.append(1.0*peopleLeft.count()/monthlyHour.count())
averageMonthlyHour.append('point_biserial=%f' % point_biserial(3))

timeSpendCompany = ['time_spend_company']
for i in range(2,11):
	spendCompany = lineOfData.filter(lambda a: int(a[4]) == i)
	# use if to avoid being divided by zero
	if spendCompany.count() == 0:
		timeSpendCompany.append(0)
	else:
		peopleLeft = spendCompany.filter(lambda a: int(a[9]) == 1)
		timeSpendCompany.append(1.0*peopleLeft.count()/spendCompany.count())
timeSpendCompany.append('point_biserial=%f' % point_biserial(4))

workAccident = ['work_accident']
for i in range(2):
	accident = lineOfData.filter(lambda a: int(a[5]) == i)
	peopleLeft = accident.filter(lambda a: int(a[9]) == 1)
	workAccident.append(1.0*peopleLeft.count()/accident.count())
workAccident.append("Cramer's V=%f" % cramer_v(5))

promotionLast5Years = ['promotion_last_5years']
for i in range(2):
	promotion = lineOfData.filter(lambda a: int(a[6]) == i)
	peopleLeft = promotion.filter(lambda a: int(a[9]) == 1)
	promotionLast5Years.append(1.0*peopleLeft.count()/promotion.count())
promotionLast5Years.append("Cramer's V=%f" % cramer_v(6))

occupation = ['occupation']
for i in ['accounting','hr','IT','management','marketing','product_mng','RandD','sales','support','technical']:
	occ = lineOfData.filter(lambda a: a[7] == i)
	peopleLeft = occ.filter(lambda a: int(a[9]) == 1)
	occupation.append(1.0*peopleLeft.count()/occ.count())
occupation.append("Cramer's V=%f" % cramer_v(7))

salary = ['salary']
for i in ['low', 'medium','high']:
	sal = lineOfData.filter(lambda a: a[8] == i)
	peopleLeft = sal.filter(lambda a: int(a[9]) == 1)
	salary.append(1.0*peopleLeft.count()/sal.count())
salary.append("Cramer's V=%f" % cramer_v(8))

# write the data into csv
with open('/home/hadoop/workspace/result.csv', 'wb') as csvfile:
	hrWriter = csv.writer(csvfile, delimiter=',')
	hrWriter.writerow(satisfactionLevel)
	hrWriter.writerow(lastEvaluation)
	hrWriter.writerow(numberProject)
	hrWriter.writerow(averageMonthlyHour)
	hrWriter.writerow(timeSpendCompany)
	hrWriter.writerow(workAccident)
	hrWriter.writerow(promotionLast5Years)
	hrWriter.writerow(occupation)
	hrWriter.writerow(salary)

