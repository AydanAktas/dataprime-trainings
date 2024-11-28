using System.Text.Json.Serialization;

public class Address
{   
    public int AddressID { get; set; }
    
    public string AddressLine1 { get; set; }

    public string AddressLine2 { get; set; }

    public string City { get; set; }

    public int StateProvinceID { get; set; }

    public string PostalCode { get; set; }

    public string SpatialLocation { get; set; }

    public string id { get; set; }

    public string ModifiedDate { get; set; }

}