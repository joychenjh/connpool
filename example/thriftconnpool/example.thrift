namespace go  example



service ConnPoolExample{
    string Ping(1: string meta)
    string Echo(1: string req)
}

