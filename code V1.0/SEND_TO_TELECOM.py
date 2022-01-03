import utils as u
import os

local_path = os.path.dirname(os.path.realpath(__file__))

remote_master='tp-4b01-10'
remote_path='/tmp/inf727_lmainguet_master'

# --Suppression du dossier master non vierge sur la machine de l'école
list_proc=u.multi_ssh([remote_master],'rm -r '+remote_path)
u.disp_diag('Supression du dossier master existant',list_proc,[remote_master],10)

# --Création d'un dossier master vierge
list_proc=u.multi_ssh([remote_master],'mkdir '+remote_path)
u.disp_diag('Recréation d\'un dossier master vierge',list_proc,[remote_master],10)

list_files=['MASTER.py','SLAVE.py','utils.py','wordcount_sequentiel.py','generate_machines_list.py','machines.txt']
list_files=[local_path+'/'+file for file in list_files]

# --Envoi des fichiers contenus dans list_files vers le master
list_proc=u.multi_scp([remote_master],list_files,remote_path,diff_files=False,mult_files_per_m=True)
u.disp_diag('Envoi code source et fichiers config vers machine master',list_proc,[remote_master],10)

print('\nATTENTION : Penser à ajouter le input.txt - A récupérer dans mon home sur la machine de l\'école')
print('Commande à utiliser : cp ~/ressources_inputs/<nom_fichier> /tmp/inf727_lmainguet_master/input.txt\n')
