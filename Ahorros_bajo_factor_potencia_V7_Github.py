# -*- coding: utf-8 -*-
import sqlalchemy
import mysql.connector as sql
import pandas as pd
#from datetime import datetime
import time
import datetime
import numpy as np
import copy
import os
import sys


Host = 'Voir Passwords_CFE_AT&T_June_2022'
DB = 'Voir Passwords_CFE_AT&T_June_2022'
User = 'Voir Passwords_CFE_AT&T_June_2022'
MDP = 'Voir Passwords_CFE_AT&T_June_2022'

db_connection = sqlalchemy.create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}'
            .format(User, MDP,Host, DB)).connect()

facturas_cfe = pd.read_sql(
"""
WITH new_cfe as 
(SELECT *,
convert(concat(anio_desde,'-', mes_desde,'-', dia_desde), DATE) as Fecha_desde,
convert(concat(anio_hasta,'-', mes_hasta,'-', dia_hasta), DATE) as Fecha_hasta,
convert(concat(anio_fac,'-', mes_fac,'-', dia_hasta), DATE) as Fecha_fac
FROM att_energia_sitios_sin_pk.facturas_cfe_09),

cfe_days as
(SELECT *, (case when cl_tarifa = 1 then 'DAC'
      when cl_tarifa = 2 then 'PDBT'
      when cl_tarifa = 3 then 'GDBT'
      when cl_tarifa between 61 and 68 then 'GDMTO'
      when cl_tarifa between 71 and 78 then 'GDMTH'
      when cl_tarifa = '5A' then 'AP'
     ELSE 'BD2' END) AS tarifa_cfe, 
     datediff(Fecha_hasta, 
     Fecha_desde) dif_days FROM new_cfe)

SELECT 
B.Fecha_desde,
B.Fecha_hasta,
B.dif_days,
(case when B.dif_days >= 35 and B.dif_days <= 65 then 'Bim'
      when B.dif_days > 0 and B.dif_days < 35 then 'Men'
ELSE 'Otro' END) as per_fac,
B.Fecha_fac,
B.periodo,
B.status,
B.ds_edo,
B.RPU,
B.ID,
C.Site_id,
B.nombre as Nombre,
B.Razon_social,
B.tarifa_cfe,
B.im_energia,
B.im_bfp,
B.im_bten,
B.im_enertot,
B.im_iva,
B.im_dap,
B.im_total,
B.cons_resu as carga_real,
B.cga_contr,
B.cga_conec,
B.demanda,
(CASE when B.tarifa_cfe = 'PDBT' then B.cons_resu/(B.dif_days*24*0.58)
      when B.tarifa_cfe = 'GDBT' then B.cons_resu/(B.dif_days*24*0.49)
      when B.tarifa_cfe = 'GDMTO' then B.cons_resu/(B.dif_days*24*0.55)
      when B.tarifa_cfe = 'GDMTH' then B.cons_resu/(B.dif_days*24*0.57)
      when B.tarifa_cfe = 'AP' then B.cons_resu/(B.dif_days*24*0.50)
      when B.tarifa_cfe = 'BD2' then cons_resu/(B.dif_days*24*0.59)
      ELSE B.cons_resu/(B.dif_days*24*0.59) END) AS demanda_cal,
ROUND((B.im_enertot + B.im_dap), 3) Total,
(B.fac_pot / 10000) as Fac_pot,
B.Owner,
B.Comments,
B.Progress
FROM cfe_days B
LEFT JOIN att_energia_sitios_sin_pk.Class_nodes C
ON B.ID = C.Tracker
WHERE B.Agrupacion = 'Sitio'
AND B.status in ('Activo', 'Baja en proceso', 'Aplica a baja');

""", con=db_connection)

# Desconectar de la base de datos
db_connection.close()


# En caso que yo quiera verlo en Excel en mi computadora
#facturas_cfe.to_excel("C:\\Users\\balle\\Desktop\\MaxiTop.xlsx")


# Consigue el camino absoluto de tu computadora
def get_desktop():
    desktop = os.path.join(os.path.join(os.environ["USERPROFILE"]),"Desktop")
    return desktop

A = facturas_cfe.sort_values(by = ["RPU","Fecha_desde"], axis=0, ascending=True)
A = A.loc[A["carga_real"] != 0]
A = A.loc[(A["im_bfp"] != 0) & (A["Fac_pot"] != 0)].reset_index(drop = True)
B = A.groupby(["RPU"]).mean()

def NewDS():
    NewColumn = []
    for values in list(np.unique(A["RPU"])):
        NewColumn.append(list(A.loc[A["RPU"] == values]["ds_edo"])[0])
    return NewColumn

Colonne1 = np.unique(A["RPU"])
Colonne2 = np.array(NewDS())  
Colonne3 = np.array(B["im_energia"])
Colonne4 = np.array(B["Fac_pot"])

# Pénalisation réelle par facteur de puissance
def Colonne5():
    L = []
    for i in range(len(B)):
        if Colonne4[i] <= 0.3:
            penal = 1.2*Colonne3[i]
            L.append(penal)
        if 0.3 <= Colonne4[i] < 0.9:
            penal = 3/5*(90/(Colonne4[i]*100) - 1)*Colonne3[i]
            L.append(penal)
        if 0.9 <= Colonne4[i]:
            bonus = -1/4*(1 - 90/(Colonne4[i]*100))*Colonne3[i]
            L.append(bonus)
    return L

Colonne5 = np.array(Colonne5())

# Economies par passage à 90
def Colonne6():
    L = []
    for i in range(len(B)):
        if Colonne5[i] >= 0:
            L.append(Colonne5[i])
        else:
            L.append(0)
    return L

Colonne6 = np.array(Colonne6())

# Economies par passage à 95
def Colonne7():
    L = []
    for i in range(len(B)):
        if Colonne4[i] <= 0.3:
            penal = 1.2*Colonne3[i] + 1/4*(1 - 90/95)*Colonne3[i]
            L.append(penal)
        elif 0.3 <= Colonne4[i] < 0.9:
            penal = 3/5*(90/(Colonne4[i]*100) - 1)*Colonne3[i] + 1/4*(1 - 90/95)*Colonne3[i]
            L.append(penal)
        elif 0.9 <= Colonne4[i] <= 0.95:
            bonus = 1/4*(1 - 90/95)*Colonne3[i] - 1/4*(1 - 90/(Colonne4[i]*100))*Colonne3[i]
            L.append(bonus)
        else:
            L.append(0)
    return L

Colonne7 = np.array(Colonne7())
    
# Economies par passage à 97            
def Colonne8():
    L = []
    for i in range(len(B)):
        if Colonne4[i] <= 0.3:
            penal = 1.2*Colonne3[i] + 1/4*(1 - 90/97)*Colonne3[i]
            L.append(penal)
        elif 0.3 <= Colonne4[i] < 0.9:
            penal = 3/5*(90/(Colonne4[i]*100) - 1)*Colonne3[i] + 1/4*(1 - 90/97)*Colonne3[i]
            L.append(penal)
        elif 0.9 <= Colonne4[i] <= 0.97:
            bonus = 1/4*(1 - 90/97)*Colonne3[i] - 1/4*(1 - 90/(Colonne4[i]*100))*Colonne3[i]
            L.append(bonus)
        else :
            L.append(0)
    return L

Colonne8 = np.array(Colonne8())

# Economies par passage à 100            
def Colonne9():
    L = []
    for i in range(len(B)):
        if Colonne4[i] <= 0.3:
            penal = 1.2*Colonne3[i] + 1/4*(1 - 90/100)*Colonne3[i]
            L.append(penal)
        elif 0.3 <= Colonne4[i] < 0.9:
            penal = 3/5*(90/(Colonne4[i]*100) - 1)*Colonne3[i] + 1/4*(1 - 90/100)*Colonne3[i]
            L.append(penal)
        elif 0.9 <= Colonne4[i] <= 1:
            bonus = 1/4*(1 - 90/100)*Colonne3[i] - 1/4*(1 - 90/(Colonne4[i]*100))*Colonne3[i]
            L.append(bonus)
        else :
            L.append(0)
    return L

Colonne9 = np.array(Colonne9())

DS_Final = pd.DataFrame()

DS_Final.insert(0,'RPU',Colonne1)
DS_Final.insert(1,'Estado',Colonne2)
DS_Final.insert(2,'Costo Energia',Colonne3)
DS_Final.insert(3,'Factor de potencia',Colonne4)
DS_Final.insert(4,'Costo actual por factor de potencia',Colonne5)
DS_Final.insert(5,'Costo con FP = 90',Colonne6)
DS_Final.insert(6,'Costo con FP = 95',Colonne7)
DS_Final.insert(7,'Costo con FP = 97',Colonne8)
DS_Final.insert(8,'Costo con FP = 100',Colonne9)


def Pres():
    Presentation = pd.DataFrame(columns = ["Estado","Numero de RPUs","Gasto actual mensual","Numero de RPUs con FP < 90","Numero de RPUs con 90 <= FP < 95","Numero de RPUs con 95 =< FP < 97","Numero de RPUs con 97 <= FP < 1","Numero de RPUs con FP = 1","Ahorro anualisado por FP = 90","Ahorro anualisado por FP = 95","Ahorro anualisado por FP = 97","Ahorro anualisado por FP = 100"])
    fin2022 = datetime.datetime.strptime("31/12/2022","%d/%m/%Y")
    Ahora = datetime.datetime.strptime(str(datetime.datetime.now().day) + "/" + str(datetime.datetime.now().month) + "/" + str(datetime.datetime.now().year),"%d/%m/%Y")
    for estado in list(np.unique(DS_Final["Estado"])):
        NumRPU = len(DS_Final.loc[DS_Final["Estado"] == estado]["RPU"])
        GActual = sum(list(DS_Final.loc[DS_Final["Estado"] == estado]["Costo actual por factor de potencia"]))
        NumRPU90 = len(DS_Final.loc[(DS_Final["Factor de potencia"] < 0.9) & (DS_Final["Estado"] == estado)])
        NumRPU95 = len(DS_Final.loc[(0.9 <= DS_Final["Factor de potencia"]) & (DS_Final["Factor de potencia"] < 0.95) & (DS_Final["Estado"] == estado)])
        NumRPU97 = len(DS_Final.loc[(0.95 <= DS_Final["Factor de potencia"]) & (DS_Final["Factor de potencia"] < 0.97) & (DS_Final["Estado"] == estado)])
        NumRPU10 = len(DS_Final.loc[(0.97 <= DS_Final["Factor de potencia"]) & (DS_Final["Factor de potencia"] < 1) & (DS_Final["Estado"] == estado)])
        NumRPU11 = len(DS_Final.loc[DS_Final["Factor de potencia"] == 1 & (DS_Final["Estado"] == estado)])
        A90 = sum(list(DS_Final.loc[DS_Final["Estado"] == estado]["Costo con FP = 90"]))*365/32
        A95 = sum(list(DS_Final.loc[DS_Final["Estado"] == estado]["Costo con FP = 95"]))*365/32
        A97 = sum(list(DS_Final.loc[DS_Final["Estado"] == estado]["Costo con FP = 97"]))*365/32
        A100 = sum(list(DS_Final.loc[DS_Final["Estado"] == estado]["Costo con FP = 100"]))*365/32
        #Presentation.append({'Estado' : estado, 'Numero de RPUs' : NumRPU, "Gasto actual" : GActual, "Numero de RPUs con FP < 90" : NumRPU90, "Numero de RPUs con 90 <= FP < 95" : NumRPU95, "Numero de RPUs con 95 =< FP < 97" : NumRPU97, "Numero de RPUs con 97 <= FP < 1" : NumRPU10, "Numero de RPUs con FP = 1" : NumRPU11, "Ahorro por 90" : A90, "Ahorro por 95" : A95, "Ahorro por 97" : A97, "Ahorro por 100" : A100},ignore_index = True)
        Presentation = pd.concat([Presentation,pd.DataFrame([[estado,NumRPU,GActual,NumRPU90,NumRPU95,NumRPU97,NumRPU10,NumRPU11,A90,A95,A97,A100]],columns = list(Presentation.columns))],ignore_index = True)
    return Presentation


DS_Final.to_excel(get_desktop() + "\\Lista_Por_RPU.xlsx",index = False)
Pres().to_excel(get_desktop() + "\\Ahorros_factor_potencia.xlsx",index = False)





