x = 50


for t in range(x//20):
    for n in range(x//9):
        for s in range(x//6):
            if ((t*20) + (n*9) + (s*6)) == x:
                print(t,n,s)
              

