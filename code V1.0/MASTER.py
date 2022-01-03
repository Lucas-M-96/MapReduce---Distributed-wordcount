#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import subprocess
import utils as u
import time
import re
import shutil
import socket

# --Efface complètement le dossier général sur les slaves pour repartir d'une situation "propre" - Initialisation
def CLEAN(list_m,prev_slave_path):

	list_proc=u.multi_ssh(list_m,'rm -rf '+prev_slave_path)
	u.disp_diag('CLEAN',list_proc,list_m,time_out_value)


# --Le master déploie tous les fichiers de fonctionnement dont auront besoin les slaves - Transfert
def DEPLOY(list_m,slave_path):
	print('DEPLOY\nDans les slaves :')

	# --Création du dossier de travail général dans le tmp de chaque slave
	list_proc=u.multi_ssh(list_m,'mkdir '+slave_path)
	u.disp_diag('	creation repertoire lmainguet dans tmp',list_proc,list_m,time_out_value)
	
	# --Liste des fichiers généraux à envoyer vers les slaves pour que le programme puisse tourner
	list_files=[master_path+'/SLAVE.py',master_path+'/utils.py',master_path+'/machines.txt']

	# --Envoi des fichiers contenus dans list_files vers les slaves
	list_proc=u.multi_scp(list_m,list_files,slave_path,diff_files=False,mult_files_per_m=True)
	u.disp_diag('	envoi des slave.py',list_proc,list_m,time_out_value)



# --Génération des splits - En local
def SPLIT(list_m,master_path,master_splt_path):

	# --Suppression d'un éventuel dossier split existant et de son contenu
	try:
		shutil.rmtree(master_splt_path)
	except :
		pass

	# --Recreation d'un nouveau dossier split
	os.mkdir(master_splt_path)

	# --Recupération du contenu du fichier input sous forme de string
	with open(master_path+'/input.txt','r') as inpt:
		inpt=inpt.read()

	# --Calcul du pas de découpe du fichier
	splt_size=int(len(inpt)/len(list_m))
	prev_cut=0

	# --Boucle contenant autant d'itérations que de splits créés
	for i in range(1,len(list_m)):
		# --On se place à la fraction du document correspondant à la fin du split et on définit la position du cut comme le premier caractère "whitespace" rencontré
		cut=i*splt_size
		while (inpt[cut] != ' ') & (inpt[cut] != '\n'):
			cut=cut+1

		# --On place la fraction du input dans un fichier split
		with open(master_splt_path+'/S'+str(i)+'.txt','w') as splt:
			splt.write(inpt[prev_cut:cut])
		prev_cut=cut+1
	
	# Creation du dernier split avec "ce qui reste" hors de la boucle
	with open(master_splt_path+'/S'+str(i+1)+'.txt','w') as splt:
		splt.write(inpt[prev_cut:])




# --Envoi des splits - Transfert
def SEND_SPLIT(list_m,master_splt_path,slave_splt_path):

	# --On récupère la liste de tous les splits
	list_splt=sorted(os.listdir(master_splt_path))

	list_proc=[]
	list_send_splt=[]

	# --Distribue les splits en plusieurs sous-listes, chaque sous-liste etant associee à une machine
	# La demarche ci-dessous de regrouper plusieurs splits par machine est inutile si jamais on s'arrange à l'avance pour n'avoir qu'un split par machine
	# Le code pourrait dans ce cas être simplifié
	list_send_splt=[[] for i in range(len(list_m))]
	for splt in list_splt:
		ind_m=list_splt.index(splt)%len(list_m) # numero de la machine sur laquelle on va envoyer le split
		list_send_splt[ind_m].append(master_splt_path+splt)
	# A voir si une étape de tar.gz vaut le coup (sur chacun de nos sous-groupes)
	
	print('SEND SPLITS')
	# --Création du sous-dossier splits sur les slaves
	list_proc=u.multi_ssh(list_m,'mkdir '+slave_splt_path)
	u.disp_diag('	creation dossier splits dans slaves',list_proc,list_m,time_out_value)

	# --Lancement des process et suivi des erreurs	
	list_proc=u.multi_scp(list_m,list_send_splt,slave_splt_path,diff_files=True,mult_files_per_m=True)
	u.disp_diag('	send splits vers les slaves',list_proc,list_m,time_out_value)




# --Etape du map - A distance
def LAUNCH_MAP(list_m,slave_path):
	list_proc=u.multi_ssh(list_m,'python3 '+slave_path+'SLAVE.py '+slave_path+' 0')
	# Ci-dessus, on envoie le slave_path en argument pour pouvoir le récupérer et l'utiliser dans le slave, qui par défaut se place dans le home
	u.disp_diag('LAUNCH_MAP',list_proc,list_m,time_out_value)
	print('MAP_FINISHED')




# --Etape du shuffle - A distance
def LAUNCH_SHUFFLE(list_m,slave_path):
	print('LAUNCH_SHUFFLE')
	
	# --Création au préalable du sous-dossier shufflesreceived sur les slaves - evite d'avoir à le faire au niveau des slaves
	list_proc=u.multi_ssh(list_m,'mkdir '+slave_path+'shufflesreceived')
	u.disp_diag('	creation dossier shufflesreceived dans slaves',list_proc,list_m,time_out_value)

	# --Ordre d'exécution du shuffle des slaves (2 étapes : calcul des hash + transfert)
	list_proc=u.multi_ssh(list_m,'python3 '+slave_path+'SLAVE.py '+slave_path+' 1')
	# Ci-dessus, on envoie le slave_path en argument pour pouvoir le récupérer et l'utiliser dans le slave, qui par défaut se place dans le home
	u.disp_diag('SHUFFLE',list_proc,list_m,time_out_value)
	print('SHUFFLE_FINISHED')



# --Etape du reduce - A distance
def LAUNCH_REDUCE(list_m,slave_path):
	list_proc=u.multi_ssh(list_m,'python3 '+slave_path+'SLAVE.py '+slave_path+' 2')
	# Ci-dessus, on envoie le slave_path en argument pour pouvoir le récupérer et l'utiliser dans le slave, qui par défaut se place dans le home
	u.disp_diag('REDUCE SLAVES',list_proc,list_m,time_out_value)



# --Envoi des résultats du reduce au master - Transfert
def SEND_REDUCE(list_m,slave_path,master_reduce_path,master_hostname):

	# --Suppression d'un éventuel dossier reduces existant sur le master
	try:
		shutil.rmtree(master_reduce_path)
	except :
		pass
	# --Creation du dossier reduce sur le master, pour collecter les résultats de tous les slaves
	os.mkdir(master_reduce_path)

	list_proc=u.multi_ssh(list_m,'python3 '+slave_path+'SLAVE.py '+slave_path+' 3 '+master_reduce_path+'/ '+master_hostname)
	# Ci-dessus, on envoie le slave_path en argument pour pouvoir le récupérer et l'utiliser dans le slave, qui par défaut se place dans le home
	u.disp_diag('SEND_REDUCE',list_proc,list_m,time_out_value)




# --Termine le reduce en local
def COMPILE_REDUCE(master_reduce_path,master_path):

	print('COMPILE_REDUCE')

	start=time.time()
	# --Récupération de la liste des reduces collectés
	list_collected_reduces=sorted(os.listdir(master_reduce_path))

	dico_res={}
	# --Récupération sous forme d'une liste du contenu de chaque fichier reduce
	for red_file in list_collected_reduces:
		with open(master_reduce_path+'/'+red_file,'r') as red_f:
			red_content=red_f.read().split()

		# --Les listes ainsi récupérées sont utilisées pour alimenter un dictionnaire global contenant tous les résultats
		for i in range(int(len(red_content)/2)):
			dico_res[red_content[2*i]]=int(red_content[2*i+1]) # Il n'y a pas de doublon sur les mots donc chaque chiffre est directement le nombre total d'occurences du mot
	duration_assembl_red=time.time()-start
	print(f'    Durée d\'assemblage des reduces dans un dictionnaire : {round(duration_assembl_red,2)}')

	# --Tri du résultat
	start=time.time()
	list_tuples_sorted=sorted(dico_res.items(),key=lambda x : (-x[1],x[0]))
	duration_tri=time.time()-start
	print(f'    Durée du tri : {round(duration_tri,2)}')

	start=time.time()
	# --Ecriture du résultat dans un fichier output.txt
	with open(master_path+'/output.txt','w') as output_f:
		for tupl in list_tuples_sorted:
				output_f.write(f"{tupl[0]} {tupl[1]} \n")
	duration_ecriture_finale=time.time()-start
	print(f'    Durée écriture finale : {round(duration_ecriture_finale,2)}')

if __name__=='__main__':

	master_hostname=socket.gethostname()
	master_path=os.path.dirname(os.path.realpath(__file__))

	with open(master_path+'/machines.txt','r') as machines:
		list_m=machines.read().splitlines()
		nb_m=len(list_m)

	prev_slave_path='/tmp/inf727_lmainguet/' # utilisé pour le clean uniquement
	slave_path='/tmp/inf727_lmainguet/'

	time_out_value=600 # A changer dans le slave aussi si modification !!

	CLEAN(list_m,prev_slave_path)
	DEPLOY(list_m,slave_path)

	start_global=time.time()

	start=time.time()
	SPLIT(list_m,master_path,master_path+'/splits')
	duration_decoupage_splt=time.time()-start
	start=time.time()
	SEND_SPLIT(list_m,master_path+'/splits/',slave_path+'splits/')
	duration_envoi_splt=time.time()-start

	start=time.time()
	LAUNCH_MAP(list_m,slave_path)
	duration_map=time.time()-start

	start=time.time()
	LAUNCH_SHUFFLE(list_m,slave_path)
	duration_shuf=time.time()-start

	start=time.time()
	LAUNCH_REDUCE(list_m,slave_path)
	duration_red_slaves=time.time()-start
	start=time.time()
	SEND_REDUCE(list_m,slave_path,master_path+'/reduces',master_hostname)
	duration_send_red=time.time()-start
	start=time.time()
	COMPILE_REDUCE(master_path+'/reduces',master_path)
	duration_compile_red=time.time()-start

	duration_tot=time.time()-start_global

	print(f'\nRECAPITULATIF PERFORMANCES ({nb_m} machines)')
	print(f'	- SPLIT : {round(duration_decoupage_splt+duration_envoi_splt,1)}')
	print(f'		Dont découpage par master : {round(duration_decoupage_splt,1)}')
	print(f'		Dont envoi vers slaves : {round(duration_envoi_splt,1)}')
	print(f'	- MAP : {round(duration_map,1)}')
	print(f'	- SHUFFLE : {round(duration_shuf,1)}')
	print(f'	- REDUCE ET COMPILATION : {round(duration_red_slaves+duration_send_red+duration_compile_red,1)}')
	print(f'		Reduce slaves : {round(duration_red_slaves,1)}')
	print(f'		Envoi reduces : {round(duration_send_red,1)}')
	print(f'		Compilation reduces : {round(duration_compile_red,1)}')
	print(f'	- TOTAL : {round(duration_tot,1)}')