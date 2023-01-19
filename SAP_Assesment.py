"""Program to find the first occurance of a missing data (strings)"""

def lostCharNW(stringSent, stringRec):
    stringSent = stringSent.lower() 
    stringRec = stringRec.lower()
    i = 0
    while len(stringRec) < len(stringSent):
        if stringSent[-2] == stringRec[-1]:
            stringRec += "x"
        else:
            lst_stringRec = list(stringRec)
            if stringSent[i] != stringRec[i]:
                idx = stringSent.index(stringSent[i])
                lst_stringRec.insert(idx - 1, "x")
            stringRec = "".join(lst_stringRec)
        i += 1
    missing_chars = list(map(lambda x, y: x if y == "x" else "", stringSent, stringRec))
    missing_chars = "".join(missing_chars)    
    return missing_chars

def main():
    try:
        #input for stringSent
        stringSent = str(input())
        # INPUT1: abcdfjgerj
        # INPUT2: aaaaaaaabaa
        # INPUT3: abcdfjgerj

        #input for stringRec
        stringRec = str(input())
        # INPUT1: abcdfjger
        # INPUT2: aaaaaaaaaa
        # INPUT3: bcdfjger
        
        result = lostCharNW(stringSent, stringRec)
        validate_results(result)
        print(result)
    except AssertionError as a:
        print(a)
def validate_results(results):
    assert results in ["aj", "b", "j"], "Output must be j, b or aj. Try again."
        
if __name__ == "__main__":
	main()