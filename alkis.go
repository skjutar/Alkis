package alkis

import (
"encoding/xml"
"fmt"
"net/http"

"appengine"
"appengine/datastore"
"appengine/urlfetch"
)


const (
    systemetURL = "http://www.systembolaget.se/Assortment.aspx?Format=Xml"
)

func defaultBeverages(c appengine.Context) *datastore.Key {
    return datastore.NewKey(c, "Beverages", "default", 0, nil)
}

type Beverage struct {
    ArticleID int64 `xml:"Artikelid"`
    ProductNumber int64 `xml:"Varnummer"`
    Name string `xml:"Namn"`
    Price string `xml:"Prisinklmoms"`
    Volume float32 `xml:"Volymiml"`
    Group string `xml:"Varugrupp"`
    Country string `xml:"Ursprunglandnamn"`
    Alcohol string `xml:"Alkoholhalt"`
}

func (b *Beverage) key(c appengine.Context) *datastore.Key {
    if b.ArticleID == 0 {
        return datastore.NewIncompleteKey(c, "Beverage", defaultBeverages(c))
    }
    return datastore.NewKey(c, "Beverage", "", b.ArticleID, defaultBeverages(c))
}

func deleteAllBeverages(c appengine.Context) (string) {
    for {
        keys, err := datastore.NewQuery("Beverage").Ancestor(defaultBeverages(c)).KeysOnly().Limit(500).GetAll(c, nil);
        if err != nil {
            return err.Error();
        }
        datastore.DeleteMulti(c, keys)
        if len(keys) < 500 { 
            return "done";
        }
    }
}


func saveMultiBeverages(c appengine.Context, beverages []Beverage) (error) {
    keys := make([]*datastore.Key, len(beverages));
    for i := 0; i < len(beverages); i++ {
        keys[i] = beverages[i].key(c);
    }
    _, err := datastore.PutMulti(c, keys, beverages);

    return err
}



func init() {
    http.HandleFunc("/cron/update", updateHandler)
    http.HandleFunc("/cron/delete", deleteHandler)
}

func deleteHandler(w http.ResponseWriter, r *http.Request) {
    c := appengine.NewContext(r)
    result := make(chan string, 1)

    go func () {
        result <- deleteAllBeverages(c)
        }()

        fmt.Fprint(w, <- result)
    }


    func updateHandler(w http.ResponseWriter, r *http.Request) {
        c := appengine.NewContext(r)
        deleteAllBeverages(c); //first, delete all current beverage
        client := urlfetch.Client(c)
        resp, err := client.Get(systemetURL);

        if err != nil {
            http.Error(w, err.Error(), http.StatusInternalServerError)
            return
        }

        batch := make([]Beverage, 0, 100)
        done := make(chan error, 1)
        var numberOfBatches int
        numberOfBatches = 0

        defer resp.Body.Close()
        decoder := xml.NewDecoder(resp.Body);

        for { 
            t, _ := decoder.Token() 
            if t == nil { 
                if len(batch) > 0 {
                    numberOfBatches++;
                    go func(beverages []Beverage)  {
                        done <- saveMultiBeverages(c, beverages);
                        }(batch)  
                    } 
            break //done
        } 
        switch se := t.(type) { 
            case xml.StartElement: 

            if se.Name.Local == "artikel" { 
                var b Beverage 
                decoder.DecodeElement(&b, &se) 
                batch = append(batch, b)
                if len(batch) == cap(batch) {
                    numberOfBatches++
                    go func(beverages []Beverage)  {
                        done <- saveMultiBeverages(c, beverages);
                        }(batch)  
                        batch = make([]Beverage, 0, 100)
                    } 
                }
            }
        }

        for i := 0; i < numberOfBatches; i++ { //wait for all batches to save
            err := <- done
            if err != nil {
                fmt.Errorf("crone error");
            }
        }
        fmt.Fprint(w, numberOfBatches); 
    }


