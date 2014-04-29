def start_block(name):
    print("##teamcity[blockOpened name='{0}']".format(name))

def end_block(name):
    print("##teamcity[blockClosed name='{0}']".format(name))

