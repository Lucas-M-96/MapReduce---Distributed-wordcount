#!/usr/bin/env python
# -*- coding: utf-8 -*-

import subprocess
import time
import numpy as np

# --Exécute une commande donnée en parallèle sur plusieurs machines, retourne la liste des objets processus en cours
def multi_ssh(list_m,remote_cmd):
	list_proc=[]
	for m in list_m:
		proc=subprocess.Popen(['ssh','lmainguet-21@'+m,remote_cmd],stdout=subprocess.PIPE,stderr=subprocess.PIPE,universal_newlines=True)
		list_proc.append(proc)
	return list_proc

# --Permet plusieurs configurations d'envoi de fichiers : un seul par machine, plusieurs machines, différents ou non, etc (fonction utilisée par multi_scp)
def pre_process_scp_files_list(list_m,files,remote_path,diff_files,mult_files_per_m):
	# --La fonction multi_scp a besoin d'une liste de listes en tant que files (permet d'envoyer un ou plusieurs fichiers, différents ou non selon la machine')
	# --Cette fonction vérifie que l'utilisateur a bien fourni les files dans le format adapté au cas de figure dans lequel il se trouve et retourne un files formatté
	
	if (diff_files==False) & (mult_files_per_m==False):
		# si on envoie le même fichier sur toutes les machines, la fonction s'attend à recevoir juste un string
		files=[[files] for i in range(len(list_m))]
	
	elif (diff_files==False) & (mult_files_per_m==True):
		# si on envoie plusieurs fichiers sur chaque machine, mais que ces fichiers envoyés sont les mêmes sur toutes les machines, la fonction attend une liste
		files=[files for i in range(len(list_m))]
	
	elif (diff_files==True) & (mult_files_per_m==False):
		# si on envoie un unique fichier sur chaque machine, mais que ces fichiers sont différents, la fonction attend une liste également (mais le traitement est différent))
		files=[[files[i]] for i in range(len(list_m))]

	else:
		# si on envoie plusieurs fichiers différents sur les différentes machines, alors on récupère directement une liste de sous-listes
		files=files
	
	return files


# --En fonction d'une liste donnée, envoie des fichiers sur différentes machines
def multi_scp(list_m,files,remote_path,diff_files,mult_files_per_m):
	
	# --Récupération de files dans la bonne forme et vérification de la cohérence du cas de figure
	files=pre_process_scp_files_list(list_m,files,remote_path,diff_files,mult_files_per_m)
	
	# --Envoi des fichiers et récupération des processus
	list_proc=[]
	for m in list_m:
		proc=subprocess.Popen(['scp',*files[list_m.index(m)],'lmainguet-21@'+m+':'+remote_path],stdout=subprocess.PIPE,stderr=subprocess.PIPE,universal_newlines=True)
		list_proc.append(proc)
	
	return list_proc


# --Attend la fin de l'exécution des processus qu'on lui donne en argument et affiche leurs sorties (standard ou erreur)
def disp_diag(nom_etape,list_proc,list_m,time_lim=None):
	# --Attend la fin de l'execution des process un a un et captage et affichage du message d'erreur si il y en a un

	print(nom_etape)
	for i in range(len(list_proc)):
		p=list_proc[i]
		m=list_m[i%len(list_m)] # Récupère le numéro de la machine si on suppose que les p sont attribués dans l'ordre aux machines
		try:
			out,err=p.communicate(timeout=time_lim)
		except subprocess.TimeoutExpired:
			print(f"    MACHINE {m} - TIMEOUT EXPIRED")
			pass
		else:
			if p.returncode != 0:
				print(f"    MACHINE {m} - Code erreur : {p.returncode} - Erreur : {err[:-1]}")
			elif p.returncode == 0:
				print(f"    MACHINE {m} : {p.returncode} {out[:-1]}")

