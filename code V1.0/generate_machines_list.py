#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys

nb_machines=int(sys.argv[1])
local_path = os.path.dirname(os.path.realpath(__file__))

# Vérifier au préalable sur le blog de la dsi que toutes les machines sont fonctionnelles !
list_num=list(range(12,12+nb_machines))

list_m=['tp-4b01-'+str(num) for num in list_num]

with open(local_path+'/machines.txt','w') as machines_f:
	for m in list_m:
		machines_f.write(m+'\n')



