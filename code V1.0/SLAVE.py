#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import re
import sys
import socket
import hashlib
import utils as u
import re
import time

# --Génération d'un unique fichier unsorted map qui contient un mot par ligne, avec un 1 associé
def MAP(splt_path,maps_path):
	
	# --Creation du dossier maps dans le SLAVE
	try:
		os.mkdir(maps_path)
	except FileExistsError:
		pass

	# --Récupération de la liste de splits présents sur le SLAVE
	list_splt=sorted(os.listdir(splt_path))

	# --Ecriture des fichiers UM dans le dossier maps du SLAVE
	for splt in list_splt:
		num_splt=(re.search('\d+',splt)).group()
		with open(splt_path+splt,'r') as splt_content:
			words=splt_content.read().split()
		# print(type(words))
		# print(words)
		words=[w.lower() for w in words]
		words=' 1\n'.join(words)
		with open(maps_path+'/UM'+num_splt+'.txt','w') as um:
			um.write(words+' 1')



# --Création en local d'un fichier shuffle unique par slave destinataire - Local
def PREP_SHUFFLE(maps_path,shuffles_path,list_m):

	start=time.time()

	# --Creation du dossier shuffles dans le SLAVE
	try:
		os.mkdir(shuffles_path)
	except FileExistsError:
		pass

	# --Récupération du hostname qui sera utilisé pour nommer les fichiers shuffles
	hostname=socket.gethostname()

	# --Récupération de la liste des maps présents sur le SLAVE
	list_maps=sorted(os.listdir(maps_path))

	# --Rassemblement des mots identiques dans un fichiers nommés avec leur hash et le hostname du slave
	for um in list_maps:
		
		# --On récupère la liste des mots contenus dans le unsorted map
		with open(maps_path+um,'r') as um_content:
			words_et_num=um_content.read().split()

		list_dict_shuffles=[{} for i in range(nb_m)]
		
		# --On traite un à un tous les mots du unsorted map
		for w in words_et_num[::2]: # on prend un élément sur deux pour ne récupérer que les mots et éviter les 1 présents sur chaque ligne
			
			# --On calcule le hashcode du mot
			hash_value=int.from_bytes(hashlib.blake2b(bytes(w,'utf-8'),digest_size=4).digest(),'big')
			# la fonction blake2b fait du hachage sur 4 octets (32 bits)
			
			# --On calcule le numéro de la machine vers laquelle le mot doit être envoyé
			ind_m=hash_value%(nb_m)

			# --Chaque machine a son dictionnaire associé dans lequel on fait le décompte des occurences des mots qui lui sont destinés de par leur hashcode
			list_dict_shuffles[ind_m][w]=list_dict_shuffles[ind_m].get(w,0)+1

		# --A partir des dictionnaires remplis précédemment, on génère un seul fichier par machine
		k=0
		for dic in list_dict_shuffles:
			with open(shuffles_path+'/'+list_m[k]+'_'+hostname+'.txt','w') as shuf_f:
				for tupl in dic.items():
					shuf_f.write(f"{tupl[0]} {tupl[1]} \n")
			k+=1



# --Envoi des shuffles - Transfert
def SEND_SHUFFLE(shuffles_path,shufflesreceived_path,time_out_value): #pas de list_m en argument, on construit le list_m à partir des noms des shuffle !

	# --Récupération de la liste des shuffles présents sur le slave
	list_shuffles=os.listdir(shuffles_path)

	# On définit les machines vers lesquels chaque shuffle doit être identifié (à partir du nom des shuffles)
	list_m=[shuffle[0:10] for shuffle in list_shuffles]

	# --On ajoute le chemin d'accès complet aux shuffle de list_shuffles
	list_shuffles=[shuffles_path+shuffle for shuffle in list_shuffles]


	# --Lancement des envois et suivi des erreurs
	list_proc=u.multi_scp(list_m,list_shuffles,shufflesreceived_path,diff_files=True,mult_files_per_m=False)
	u.disp_diag(f'SEND SHUFFLE TO',list_proc,list_m,time_out_value)




def REDUCE(shufflesreceived_path,reduce_path):

	# --Creation du dossier reduce dans le SLAVE
	try:
		os.mkdir(reduce_path)
	except FileExistsError:
		pass

	# --Récupération du hostname qui donnera son nom au reduce
	hostname=socket.gethostname()

	# --Récupération de la liste des shufflesreceived présents sur le slave
	list_shufflesreceived=os.listdir(shufflesreceived_path)
	list_shufflesreceived=[shufflesreceived_path+shufflereceived for shufflereceived in list_shufflesreceived]

	dico_red={}
	# --On lit le contenu de tous les shufflereceived un à un et on les convertit en listes qui comprennent une alternance de mot et de nombre d'occurence
	for shuffle_received in list_shufflesreceived:
		with open(shuffle_received,'r') as shuf_f:
			shuf_content=shuf_f.read().split()
		
		# --Au fur et à mesure des shuffles received, on continue à alimenter un même dictionnaire
		for i in range(int(len(shuf_content)/2)):
			dico_red[shuf_content[2*i]]=dico_red.get(shuf_content[2*i],0)+int(shuf_content[2*i+1])

	list_tuples_sorted=sorted(dico_red.items(),key=lambda x : (-x[1],x[0]))

	# --Pour vérifier que la répartition de la charge entre les slaves est bien équilibrée
	nb_mots=len(list_tuples_sorted)
	nb_occurences=0
	
	# --Ecriture du contenu du dictionnaire obtenu dans un fichier texte, en utilisant des espaces et des retour à la ligne comme séparateurs
	with open(reduce_path+'/'+hostname+'.txt','w') as red_f:
		for tupl in list_tuples_sorted:
				red_f.write(f"{tupl[0]} {tupl[1]} \n")
				nb_occurences+=tupl[1]
	print(f"nb mots traités : {nb_mots} - nb total occurences : {nb_occurences}")




def SEND_REDUCE(slave_reduce_path,master_reduce_path,master_hostname,time_out_value):

	# --Récupération de la liste des reduce présents sur le slave (il n'y en a normalement plus qu'un)
	list_reduce=os.listdir(slave_reduce_path)
	list_reduce=[slave_reduce_path+reduce_file for reduce_file in list_reduce]

	# --Lancement des envois et suivi des erreurs (on aurait pu se contente d'un scp simple sans passer par le subprocess du utils)
	list_proc=u.multi_scp([master_hostname],list_reduce,master_reduce_path,diff_files=False,mult_files_per_m=True) # le deuxième paramètre vaut True car même si il n'y en a qu'un, j'ai quand meme mis le nom du fichier reduce dans une liste
	u.disp_diag(f'Envoie vers',list_proc,[master_hostname],time_out_value)




if __name__=='__main__':
	
	time_out_value=600
	
	slave_path=sys.argv[1] #On a besoin du slave_path car sinon  le slave se place dans le home par défaut

	if sys.argv[2]=='0':
		MAP(slave_path+'splits/',slave_path+'maps') # le os.mkdir ne marche pas si on rajoute un / à la fin
	
	if sys.argv[2]=='1':
		# --Les slaves ont besoin  de la liste des machines pour procéder à l'envoi lors du shuffle
		with open(slave_path+'machines.txt','r') as machines:
			list_m=machines.read().splitlines()
			nb_m=len(list_m)
		
		start=time.time()
		PREP_SHUFFLE(slave_path+'maps/',slave_path+'shuffles',list_m) # le os.mkdir ne marche pas si on rajoute un / à la fin
		duration_prep_shuffles=time.time()-start
		print(f'Durée préparation shuffle : {round(duration_prep_shuffles,2)}',end=' - ')

		start=time.time()
		SEND_SHUFFLE(slave_path+'shuffles/',slave_path+'shufflesreceived/',time_out_value) # pas de os.mkdir ici
		duration_send_shuffles=time.time()-start
		print(f'Durée envoi shuffle : {round(duration_send_shuffles,2)}')

	if sys.argv[2]=='2':
		REDUCE(slave_path+'shufflesreceived/',slave_path+'reduces') # le os.mkdir ne marche pas si on rajoute un / à la fin

	if sys.argv[2]=='3':
		master_reduce_path=sys.argv[3]
		master_hostname=sys.argv[4]
		SEND_REDUCE(slave_path+'reduces/',master_reduce_path,master_hostname,time_out_value)