import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from control import *
from config import *
from datetime import datetime, timedelta
from training import train
from testing import test
from push import push

if __name__ == "__main__":
    if mode == 'train':
        train()
    elif mode == 'test':
        test()
    elif mode == 'push':
        push()

