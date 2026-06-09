import os
os.system("npm install jsonwebtoken")
os.system("node -e \"const jwt=require('jsonwebtoken'); console.log(jwt.sign({company:'Lanzi'}, 'have2025secure'));\" > token.txt")
os.system("python ../test_live_api_py.py")
