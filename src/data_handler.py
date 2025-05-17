import pandas as pd
import numpy as np
import math
from models.export import app, db, Sensor
sensors = Sensor.query.all()
sensorSeries=pd.Series([sensor.segment for sensor in sensors], index=[sensor.id for sensor in sensors])


def get3PhasePower(Va: pd.Series, Vb: pd.Series, Vc: pd.Series, Ia: pd.Series, Ib: pd.Series, Ic: pd.Series ):
    return Va*Ia + Vb*Ib + Vc*Ic

def getVISensorDataFrame(voltageCurrentList):
    VISensorDataFrame = pd.DataFrame(voltageCurrentList)
    VISensorDataFrame = VISensorDataFrame.assign(S=get3PhasePower(VISensorDataFrame['Va'], VISensorDataFrame['Vb'],\
        VISensorDataFrame['Vc'], VISensorDataFrame['Ia'], VISensorDataFrame['Ib'], VISensorDataFrame['Ic']))
    return VISensorDataFrame

def getApparentPower(realPower: pd.Series, activePower: pd.Series ):
    return pd.Series(np.sqrt((realPower**2 + activePower**2).to_numpy()))

def getDCPowerDataFrame(powerList):
    powerDataFrame = pd.DataFrame(powerList)
    powerDataFrame = powerDataFrame.assign(Sfeed=getApparentPower(powerDataFrame['Pfeed'], powerDataFrame['Qfeed']))
    return powerDataFrame

def getSegmentFeedDataFrame(VISensors: pd.DataFrame, DCPowers: pd.DataFrame):
    powersDataFrame = DCPowers.loc[:, ['timestamp', 'Sfeed']]
    VISensorGroupBy = VISensors.groupby('sensorId')
    i = 1
    for sensorId, indices in VISensorGroupBy.groups.items():
        sensorPowers = (VISensors.loc[indices, 'S'])
        sensorPowers.index=[n for n in range(len(indices))]
        powersDataFrame[f'S{sensorId}'] = sensorPowers
        if(sensorId[-1] == '1'):
            powersDataFrame[f'S{sensorId[-1]}'] = powersDataFrame['Sfeed'] - powersDataFrame[f'S{sensorId}']
        else:
            powersDataFrame[f'S{sensorId[-1]}'] = powersDataFrame[f'S{sensorId[:-1]+str(int(sensorId[-1])-1)}'] - powersDataFrame[f'S{sensorId}']
        
        if(i == len(VISensorGroupBy.groups)):
            powersDataFrame[f'S{i+1}'] = powersDataFrame[f'S{sensorId}']
        
        i += 1

    return powersDataFrame

def getSMDataFrame(smartMeterList):
    SMDataFrame = pd.DataFrame(smartMeterList)
    return SMDataFrame

def getSegmentConsuptionDataFrame(SMDataFrame):
    #SMDataFrame['sensorId'] = pd.Series([sensorSeries[id] for id in SMDataFrame['sensorId']])
    SMDataFrame = SMDataFrame.assign(S=getApparentPower(SMDataFrame['P'], SMDataFrame['Q']))
    SMGroupBy = SMDataFrame.groupby('sensorId')
    SegmentConsumptionDF = pd.DataFrame()
    for sensorId, indices  in SMGroupBy.groups.items():
        SMPower = SMDataFrame.loc[indices, ['timestamp', 'S']]
        SMPower.index = [n for n in range(len(indices))]
        SMPower = SMPower.rename({'timestamp': f'T{sensorId}', 'S': sensorId}, axis='columns')
        SegmentConsumptionDF = pd.concat([SegmentConsumptionDF, SMPower], axis=1)

    for column in SegmentConsumptionDF.columns:
        if sensorSeries.get(column):
            try:
                SegmentConsumptionDF[sensorSeries[column].upper()] = \
                    SegmentConsumptionDF[sensorSeries[column].upper()] + SegmentConsumptionDF[column]
            except KeyError:
                SegmentConsumptionDF[sensorSeries[column].upper()] = SegmentConsumptionDF[column]

    return SegmentConsumptionDF

def getPowerLossDF(segmentFeedDF: pd.DataFrame,segmentConsumptionDF: pd.DataFrame, dataConcentratorId: str):
    powerLossDF = pd.DataFrame(columns=['timestamp', 'feeder' , 'segment', 'loss'])

    for column in segmentFeedDF.columns:
        try:
            segmentLossDF= pd.DataFrame({
                'timestamp': segmentFeedDF['timestamp'],
                'feeder': pd.Series(['f'+dataConcentratorId[-1] for n in range(5)]),
                'segment': pd.Series([column.lower() for n in range(5)]),
                'loss': abs(segmentFeedDF[column] - segmentConsumptionDF[column])})
            powerLossDF = pd.concat([ powerLossDF, segmentLossDF], ignore_index=True )

        except KeyError:
            pass

    return powerLossDF

def getSegmentPowers(dataConcentrator):
    return dataConcentrator
