from typing import List, Tuple
from psycopg2 import sql
from datetime import date, datetime

import Utility.DBConnector as Connector
from Utility.ReturnValue import ReturnValue
from Utility.Exceptions import DatabaseException

from Business.Owner import Owner
from Business.Customer import Customer
from Business.Apartment import Apartment


# ---------------------------------- CRUD API: ----------------------------------

def create_tables():
    conn = None
    try:
        conn = Connector.DBConnector()
        conn.execute("""
            BEGIN;
            
            DROP TABLE IF EXISTS Owner CASCADE;
            CREATE TABLE Owner(
                owner_id INTEGER NOT NULL,
                name TEXT NOT NULL,
                PRIMARY KEY(owner_id),
                CHECK(owner_id > 0)
            );
            
            DROP TABLE IF EXISTS Apartment CASCADE;
            CREATE TABLE Apartment(
                id INTEGER NOT NULL,
                address TEXT NOT NULL,
                city TEXT NOT NULL,
                country TEXT NOT NULL,
                city_country TEXT NOT NULL,
                size INTEGER NOT NULL,
                CHECK(id > 0), CHECK(size > 0),
                PRIMARY KEY(id),
                UNIQUE(address, city)
            );
            
            DROP TABLE IF EXISTS Customer CASCADE;
            CREATE TABLE Customer(
                customer_id INTEGER NOT NULL,
                customer_name TEXT NOT NULL,
                PRIMARY KEY(customer_id),
                CHECK(customer_id > 0)
            );
            
            DROP TABLE IF EXISTS CustomerReservations CASCADE;
            CREATE TABLE CustomerReservations(
                customer_id INTEGER NOT NULL, 
                apartment_id INTEGER NOT NULL,
                start_date DATE NOT NULL,
                end_date DATE NOT NULL,
                total_price DOUBLE PRECISION NOT NULL,
                CHECK(total_price > 0),
                CHECK(end_date > start_date),
                FOREIGN KEY(customer_id) REFERENCES Customer(customer_id) ON DELETE CASCADE,
                FOREIGN KEY(apartment_id) REFERENCES Apartment(id) ON DELETE CASCADE
            );

            DROP TABLE IF EXISTS CustomerReviews CASCADE;
            CREATE TABLE CustomerReviews(
                customer_id INTEGER NOT NULL,
                apartment_id INTEGER NOT NULL,
                review_date DATE NOT NULL,
                rating INTEGER NOT NULL,
                review_text TEXT NOT NULL,
                CHECK (rating BETWEEN 1 AND 10),
                FOREIGN KEY(customer_id) REFERENCES Customer(customer_id) ON DELETE CASCADE,
                FOREIGN KEY(apartment_id) REFERENCES Apartment(id) ON DELETE CASCADE,
                UNIQUE(customer_id, apartment_id)
            );


            DROP TABLE IF EXISTS ApartmentOwners CASCADE;
            CREATE TABLE ApartmentOwners(
                owner_id INTEGER NOT NULL,
                apartment_id INTEGER NOT NULL,
                PRIMARY KEY(apartment_id),
                FOREIGN KEY(owner_id) REFERENCES Owner(owner_id) ON DELETE CASCADE,
                FOREIGN KEY(apartment_id) REFERENCES Apartment(id) ON DELETE CASCADE
            );
            
            CREATE VIEW ApartmentOwnersWithName AS
            SELECT A.owner_id AS owner_id, apartment_id, name
            FROM ApartmentOwners A JOIN Owner O ON (A.owner_id=O.owner_id);
            
            CREATE VIEW ApartmentOwnersFullData AS
            SELECT *
            FROM ApartmentOwnersWithName A RIGHT OUTER JOIN Apartment B ON(A.apartment_id = B.id);
            
            CREATE VIEW ApartmentReviewsFullData AS
            SELECT owner_id, A.id AS apartment_id, name, customer_id, review_date, rating, review_text
            FROM ApartmentOwnersFullData A JOIN CustomerReviews C ON (A.id = C.apartment_id);
            
            CREATE VIEW ApartmentAvgRating AS
            SELECT owner_id, apartment_id, AVG(rating) AS avg_rating
            FROM ApartmentReviewsFullData
            GROUP BY apartment_id, owner_id;
            
            CREATE VIEW OwnerAvgRating AS
            SELECT owner_id, AVG(avg_rating) AS avg_rating
            FROM ApartmentAvgRating
            GROUP BY owner_id;
            
            CREATE VIEW OwnerCustomerReservations AS
            SELECT owner_id, name AS owner_name, customer_id, A.apartment_id AS apartment_id 
            FROM ApartmentOwnersWithName A RIGHT OUTER JOIN CustomerReservations C ON (A.apartment_id=C.apartment_id);
            
            CREATE VIEW CustomerReservationsWithPricePerNight AS
            SELECT 
                customer_id,
                apartment_id,
                start_date,
                end_date,
                total_price,
                total_price / (end_date - start_date) AS price_per_night
            FROM CustomerReservations;

            CREATE VIEW ApartmentPriceRatingAVG AS
            SELECT A.apartment_id AS apartment_id, price_per_night, rating
            FROM CustomerReservationsWithPricePerNight C LEFT OUTER JOIN ApartmentReviewsFullData A ON (C.apartment_id=A.apartment_id);
            
            CREATE VIEW ApartmentPriceRatingAvgFullData AS
            SELECT *
            FROM ApartmentPriceRatingAVG A JOIN Apartment B ON (A.apartment_id = B.id);
            COMMIT;
            
            CREATE VIEW CustomerReviewsProd AS
            SELECT A.customer_id AS customer_a_id,
                   B.customer_id AS customer_b_id,
                   A.apartment_id AS apartment_id,
                   A.rating AS customer_a_rating,
                   B.rating AS customer_b_rating
            FROM CustomerReviews A, CustomerReviews B
            WHERE A.customer_id != B.customer_id AND A.apartment_id = B.apartment_id;
            
            CREATE VIEW CustomerReviewsAvgRatio AS
            SELECT customer_a_id, customer_b_id, AVG(customer_b_rating*1.0/customer_a_rating) AS avg_ratio
            FROM CustomerReviewsProd A
            GROUP BY customer_a_id, customer_b_id;
            

            
            
        """)

    except (DatabaseException.ConnectionInvalid, DatabaseException.database_ini_ERROR,
                DatabaseException.UNKNOWN_ERROR) as e:
        print(e)
        return ReturnValue.ERROR
    except Exception as e:
        print(e)
        return ReturnValue.ERROR
    finally:
        conn.close()


def clear_tables():
    conn = None
    try:
        conn = Connector.DBConnector()
        conn.execute("Begin;\n" +
                     "DELETE FROM Owner;\n" +
                     "DELETE FROM Apartment;\n" +
                     "DELETE FROM Customer;\n" +
                     "DELETE FROM CustomerReservations;\n" +
                     "DELETE FROM CustomerReviews;\n" +
                     "DELETE FROM ApartmentOwners;\n" +
                     "COMMIT;")
    except (DatabaseException.ConnectionInvalid, DatabaseException.database_ini_ERROR,
            DatabaseException.UNKNOWN_ERROR) as e:
        return ReturnValue.ERROR
    except Exception as e:
        return ReturnValue.ERROR
    finally:
        conn.close()


def drop_tables():
    conn = None
    try:
        conn = Connector.DBConnector()
        conn.execute("Begin;\n" +
                     "DROP TABLE IF EXISTS Owner CASCADE; \n" +
                     "DROP TABLE IF EXISTS Apartment CASCADE; \n" +
                     "DROP TABLE IF EXISTS Customer CASCADE; \n" +
                     "DROP TABLE IF EXISTS CustomerReservations CASCADE; \n" +
                     "DROP TABLE IF EXISTS CustomerReviews CASCADE; \n" +
                     "DROP TABLE IF EXISTS ApartmentOwners CASCADE; \n" +
                     "DROP VIEW IF EXISTS ApartmentOwnersWithName CASCADE; \n" +
                     "COMMIT;")
    except (DatabaseException.ConnectionInvalid, DatabaseException.database_ini_ERROR,
            DatabaseException.UNKNOWN_ERROR) as e:
        return ReturnValue.ERROR
    except Exception as e:
        return ReturnValue.ERROR
    finally:
        conn.close()


def add_owner(owner: Owner) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        owner_id = owner.get_owner_id()
        name = owner.get_owner_name()
        query = sql.SQL("INSERT INTO Owner(owner_id,name) values({owner_id} , {name});").format(owner_id=sql.Literal(owner_id), name = sql.Literal(name))
        conn.execute(query)
        conn.commit()
    except (DatabaseException.NOT_NULL_VIOLATION,DatabaseException.CHECK_VIOLATION) as e:
        print(e)
        return ReturnValue.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION as e:
        print(e)
        return ReturnValue.ALREADY_EXISTS
    except Exception as e:
        print(e)
        return ReturnValue.ERROR
    finally:
        conn.close()

    return ReturnValue.OK

def res_to_owner(res: Connector.ResultSet) -> Owner:
    return Owner(res[0]['owner_id'], res[0]['name'])

def get_owner(owner_id: int) -> Owner:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT * " +
                        "FROM Owner " +
                        "WHERE owner_id = {owner_id};").format(owner_id = sql.Literal(owner_id))
        rows_affected, res = conn.execute(query)
        # If the result of the query returned empty table it means that an owner with the requested id does not exist
        if not rows_affected:
            return Owner.bad_owner()
        conn.commit()
    except:
        return Owner.bad_owner()
    finally:
        conn.close()

    # Return the object of the requested owner
    return res_to_owner(res)


def delete_owner(owner_id: int) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("DELETE FROM Owner WHERE owner_id = {owner_id};").format(owner_id=sql.Literal(owner_id))
        rows_affected, res = conn.execute(query)
        if not rows_affected:
            return ReturnValue.NOT_EXISTS
        conn.commit()

    except (DatabaseException.NOT_NULL_VIOLATION, DatabaseException.CHECK_VIOLATION)  as e:
        print(e)
        return ReturnValue.BAD_PARAMS

    except Exception as e:
        print(e)
        return ReturnValue.ERROR

    finally:
        conn.close()

    return ReturnValue.OK



def add_apartment(apartment: Apartment) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        id = apartment.get_id()
        address = apartment.get_address()
        city = apartment.get_city()
        country = apartment.get_country()
        city_country = f"{city}_{country}"
        size = apartment.get_size()
        query = sql.SQL("INSERT INTO Apartment(id, address, city, country,city_country, size) values({id}, " +
                        "{address}, {city}, {country}, {city_country},{size});") \
            .format(id=sql.Literal(id),
                    address=sql.Literal(address),
                    city=sql.Literal(city),
                    country=sql.Literal(country),
                    city_country=sql.Literal(city_country),
                    size=sql.Literal(size))
        conn.execute(query)
        conn.commit()
    except (DatabaseException.NOT_NULL_VIOLATION,DatabaseException.CHECK_VIOLATION) as e:
        return ReturnValue.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION as e:
        return ReturnValue.ALREADY_EXISTS
    except Exception as e:
        return ReturnValue.ERROR
    finally:
        conn.close()

    return ReturnValue.OK

def res_to_apartment(res: Connector.ResultSet) -> Apartment:
    return Apartment(res[0]['id'], res[0]['address'], res[0]['city'], res[0]['country'], res[0]['size'])

def get_apartment(apartment_id: int) -> Apartment:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT * " +
                        "FROM Apartment " +
                        "WHERE id = {apartment_id};").format(apartment_id = sql.Literal(apartment_id))
        rows_affected, res = conn.execute(query)
        # If the result of the query returned empty table it means that an apartment with the requested id does not exist
        if not rows_affected:
            return Apartment.bad_apartment()
        conn.commit()
    except:
        return Apartment.bad_apartment()
    finally:
        conn.close()

    # Return the object of the requested Apartment
    return res_to_apartment(res)


def delete_apartment(apartment_id: int) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("DELETE FROM Apartment WHERE id = {apartment_id};").format(apartment_id=sql.Literal(apartment_id))
        rows_affected, res = conn.execute(query)
        if not rows_affected:
            return ReturnValue.NOT_EXISTS
        conn.commit()
    except (DatabaseException.NOT_NULL_VIOLATION, DatabaseException.CHECK_VIOLATION)  as e:
        print(e)
        return ReturnValue.BAD_PARAMS
    except Exception as e:
        return ReturnValue.ERROR
    finally:
        conn.close()

    return ReturnValue.OK


def add_customer(customer: Customer) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        customer_id = customer.get_customer_id()
        customer_name = customer.get_customer_name()
        query = sql.SQL("INSERT INTO Customer(customer_id, customer_name) values({customer_id}, {customer_name});") \
            .format(customer_id=sql.Literal(customer_id),
                    customer_name=sql.Literal(customer_name))
        conn.execute(query)
        conn.commit()
    except (DatabaseException.NOT_NULL_VIOLATION, DatabaseException.CHECK_VIOLATION) as e:
        return ReturnValue.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION as e:
        return ReturnValue.ALREADY_EXISTS
    except Exception as e:
        return ReturnValue.ERROR
    finally:
        conn.close()

    return ReturnValue.OK

def res_to_customer(res: Connector.ResultSet) -> Customer:
    return Customer(res[0]['customer_id'], res[0]['customer_name'])

def get_customer(customer_id: int) -> Customer:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT * " +
                        "FROM Customer " +
                        "WHERE customer_id = {customer_id};").format(customer_id=sql.Literal(customer_id))
        rows_affected, res = conn.execute(query)
        # If the result of the query returned empty table it means that a customer with the requested id does not exist
        if not rows_affected:
            return Customer.bad_customer()
        conn.commit()
    except:
        return Customer.bad_customer()
    finally:
        conn.close()

    # Return the object of the requested Customer
    return res_to_customer(res)


def delete_customer(customer_id: int) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("DELETE FROM Customer WHERE customer_id = {customer_id};").format(customer_id=sql.Literal(customer_id))
        rows_affected, res = conn.execute(query)
        if not rows_affected:
            return ReturnValue.NOT_EXISTS
        conn.commit()
    except (DatabaseException.NOT_NULL_VIOLATION, DatabaseException.CHECK_VIOLATION) as e:
        return ReturnValue.BAD_PARAMS
    except Exception as e:
        return ReturnValue.ERROR
    finally:
        conn.close()

    return ReturnValue.OK


def customer_made_reservation(customer_id: int, apartment_id: int, start_date: date, end_date: date,
                              total_price: float) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("""
            INSERT INTO CustomerReservations(customer_id, apartment_id, start_date, end_date, total_price)
            SELECT {customer_id}, {apartment_id}, {start_date}, {end_date}, {total_price}
            WHERE NOT EXISTS(
                SELECT 1
                FROM CustomerReservations
                WHERE apartment_id = {apartment_id} 
                AND ( ({start_date} >= start_date AND {end_date} <= end_date) OR 
                          ({end_date} >= start_date AND {end_date} <= end_date) OR
                          ({start_date} >= start_date AND {start_date} <= end_date))
                
            );
        """).format(
                  customer_id=sql.Literal(customer_id),
                  apartment_id=sql.Literal(apartment_id),
                  start_date=sql.Literal(start_date),
                  end_date=sql.Literal(end_date),
                  total_price=sql.Literal(total_price))
        rows_affected, _ = conn.execute(query)
        # Check if the apartment isn't available at the specified date
        if not rows_affected:
            return ReturnValue.BAD_PARAMS
        conn.commit()
    except (DatabaseException.NOT_NULL_VIOLATION, DatabaseException.CHECK_VIOLATION) as e:
        return ReturnValue.BAD_PARAMS
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        return ReturnValue.NOT_EXISTS
    except Exception as e:
        print(e)
        return ReturnValue.ERROR

    finally:
        conn.close()

    return ReturnValue.OK


def customer_cancelled_reservation(customer_id: int, apartment_id: int, start_date: date) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("DELETE FROM CustomerReservations WHERE customer_id = {customer_id} AND apartment_id = {apartment_id} AND start_date = {start_date};") \
            .format(customer_id=sql.Literal(customer_id),
                    apartment_id=sql.Literal(apartment_id),
                    start_date=sql.Literal(start_date))
        rows_affected, res = conn.execute(query)
        if not rows_affected:
            return ReturnValue.NOT_EXISTS
        conn.commit()

    except DatabaseException.NOT_NULL_VIOLATION as e:
        return ReturnValue.BAD_PARAMS
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        return ReturnValue.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION as e:
        return ReturnValue.BAD_PARAMS
    except Exception as e:
        return ReturnValue.ERROR

    finally:
        conn.close()

    return ReturnValue.OK


def customer_reviewed_apartment(customer_id: int, apartment_id: int, review_date: date, rating: int,
                                review_text: str) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("""
            INSERT INTO CustomerReviews(customer_id, apartment_id, review_date, rating, review_text)
            SELECT {customer_id}, {apartment_id}, {review_date}, {rating}, {review_text}
            WHERE EXISTS(
                SELECT 1
                FROM CustomerReservations
                WHERE apartment_id = {apartment_id} 
                AND ( {review_date} >= end_date)
            );
        """).format(customer_id=sql.Literal(customer_id),
                    apartment_id=sql.Literal(apartment_id),
                    review_date=sql.Literal(review_date),
                    rating=sql.Literal(rating),
                    review_text=sql.Literal(review_text))
        rows_affected, _ = conn.execute(query)
        # Check if the apartment isn't available at the specified date
        if not rows_affected:
            return ReturnValue.NOT_EXISTS
        conn.commit()
    except (DatabaseException.NOT_NULL_VIOLATION, DatabaseException.CHECK_VIOLATION) as e:
        return ReturnValue.BAD_PARAMS
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        return ReturnValue.NOT_EXISTS
    except Exception as e:
        return ReturnValue.ERROR

    finally:
        conn.close()

    return ReturnValue.OK

def customer_updated_review(customer_id: int, apartment_id:int, update_date: date, new_rating: int, new_text: str) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("""
            UPDATE CustomerReviews
            SET review_date = {update_date}, rating = {new_rating}, review_text = {new_text}
            WHERE customer_id = {customer_id} AND apartment_id = {apartment_id} AND review_date <= {update_date};
        """).format(customer_id=sql.Literal(customer_id),
                    apartment_id=sql.Literal(apartment_id),
                    update_date=sql.Literal(update_date),
                    new_rating=sql.Literal(new_rating),
                    new_text=sql.Literal(new_text))
        rows_affected, _ = conn.execute(query)
        # Check if the customer has no previous review for the apartment
        if not rows_affected:
            return ReturnValue.NOT_EXISTS
        conn.commit()
    except (DatabaseException.NOT_NULL_VIOLATION, DatabaseException.CHECK_VIOLATION) as e:
        return ReturnValue.BAD_PARAMS
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        return ReturnValue.NOT_EXISTS
    except Exception as e:
        return ReturnValue.ERROR

    finally:
        conn.close()

    return ReturnValue.OK

def owner_owns_apartment(owner_id: int, apartment_id: int) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("INSERT INTO ApartmentOwners(owner_id, apartment_id) values({owner_id}, {apartment_id});") \
            .format(owner_id=sql.Literal(owner_id),
                    apartment_id=sql.Literal(apartment_id))
        conn.execute(query)
        conn.commit()
    except DatabaseException.NOT_NULL_VIOLATION as e:
        return ReturnValue.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION as e:
        return ReturnValue.BAD_PARAMS
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        return ReturnValue.NOT_EXISTS
    except DatabaseException.UNIQUE_VIOLATION as e:
        return ReturnValue.ALREADY_EXISTS

    except Exception as e:
        return ReturnValue.ERROR

    finally:
        conn.close()

    return ReturnValue.OK


def owner_doesnt_own_apartment(owner_id: int, apartment_id: int) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("DELETE FROM ApartmentOwners WHERE owner_id = {owner_id} AND apartment_id = {apartment_id};") \
            .format(owner_id=sql.Literal(owner_id),
                    apartment_id=sql.Literal(apartment_id))
        rows_affected, res = conn.execute(query)
        if not rows_affected:
            return ReturnValue.NOT_EXISTS
        conn.commit()
    except (DatabaseException.NOT_NULL_VIOLATION, DatabaseException.FOREIGN_KEY_VIOLATION, DatabaseException.CHECK_VIOLATION) as e:
        return ReturnValue.BAD_PARAMS
    except Exception as e:
        return ReturnValue.ERROR
    finally:
        conn.close()

    return ReturnValue.OK


def get_apartment_owner(apartment_id: int) -> Owner:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("""
                    SELECT owner_id, name
                    FROM ApartmentOwnersWithName
                    WHERE apartment_id = {apartment_id};
                """).format(apartment_id = sql.Literal(apartment_id))

        rows_affected, res = conn.execute(query)
        # If the result of the query returned empty table it means that an apartment with the requested id does not exist
        if not rows_affected:
            return Owner.bad_owner()
        conn.commit()
    except:
        return Owner.bad_owner()
    finally:
        conn.close()

    # Return the object of the requested Owner
    return res_to_owner(res)

def get_owner_apartments(owner_id: int) -> List[Apartment]:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("""
                    SELECT id, address, city, country, size
                    FROM ApartmentOwnersFullData
                    WHERE owner_id = {owner_id};
                """).format(owner_id=sql.Literal(owner_id))

        rows_affected, res = conn.execute(query)
        if not rows_affected:
            return []
        conn.commit()
    except:
        return []
    finally:
        conn.close()

    apartments_list = []
    for i in range(len(res.rows)):
        apartments_list.append(Apartment(*res.rows[i]))
    return apartments_list


# ---------------------------------- BASIC API: ----------------------------------

def get_apartment_rating(apartment_id: int) -> float:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("""
                    SELECT avg_rating
                    FROM ApartmentAvgRating
                    WHERE apartment_id = {apartment_id};
                """).format(apartment_id = sql.Literal(apartment_id))

        rows_affected, res = conn.execute(query)
        if not rows_affected:
            return 0.0
        conn.commit()
    except:
        return 0.0
    finally:
        conn.close()

    return float(res[0]['avg_rating'])


def get_owner_rating(owner_id: int) -> float:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("""
                    SELECT avg_rating
                    FROM OwnerAvgRating
                    WHERE owner_id = {owner_id};
                """).format(owner_id = sql.Literal(owner_id))

        rows_affected, res = conn.execute(query)
        if not rows_affected:
            return 0.0
        conn.commit()
    except:
        return 0.0
    finally:
        conn.close()

    return float(res[0]['avg_rating'])


def get_top_customer() -> Customer:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("""
                    SELECT customer_id
                    FROM OwnerCustomerReservations
                    GROUP BY customer_id
                    ORDER BY COUNT(*) DESC, customer_id ASC
                    LIMIT 1;
                """).format()

        rows_affected, res = conn.execute(query)
        if not rows_affected:
            return Customer.bad_customer()
        conn.commit()
    except:
        return Customer.bad_customer()
    finally:
        conn.close()

    return res[0]['customer_id']


def reservations_per_owner() -> List[Tuple[str, int]]:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("""
                    SELECT owner_name, COUNT(*) AS reservations
                    FROM OwnerCustomerReservations
                    GROUP BY owner_name, owner_id;
                """).format()

        rows_affected, res = conn.execute(query)
        if not rows_affected:
            return []
        conn.commit()
    except:
        return []
    finally:
        conn.close()

    return res.rows


# ---------------------------------- ADVANCED API: ----------------------------------
# Todo: currently not fully counting unique (city, country) pairs in ApartmentOwnersFullData
def get_all_location_owners() -> List[Owner]:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("""
                    SELECT owner_id, name
                    FROM ApartmentOwnersFullData
                    GROUP BY owner_id, name
                    HAVING COUNT (DISTINCT city_country) = (
                        SELECT COUNT(DISTINCT city_country)
                        FROM Apartment
                    );
                """).format()

        rows_affected, res = conn.execute(query)
        if not rows_affected:
            return []
        conn.commit()
    except Exception as e:
        print(e)
        return []
    finally:
        conn.close()

    all_location_owners = []
    for row in res:
        all_location_owners.append(Owner(row['owner_id'], row['name']))
    return all_location_owners


def best_value_for_money() -> Apartment:
    conn = None
    try:
        conn = Connector.DBConnector()
        # We can use the aggregation funcitons here as the apartment id has one address and one city etc...
        query = sql.SQL("""
                    SELECT 
                        apartment_id AS id, 
                        MAX(address) AS address, 
                        MAX(city) AS city, 
                        MAX(country) AS country, 
                        MAX(size) AS size, 
                        AVG(rating) / AVG(price_per_night) AS value_for_money
                    FROM 
                        ApartmentPriceRatingAvgFullData
                    GROUP BY 
                        apartment_id
                    ORDER BY 
                        value_for_money DESC
                    LIMIT 1;
                """).format()
        rows_affected, res = conn.execute(query)
        if not rows_affected:
            return Apartment.bad_apartment()
        conn.commit()
    except Exception as e:
        print(e)
        return Apartment.bad_apartment()
    finally:
        conn.close()
    return res_to_apartment(res)


def profit_per_month(year: int) -> List[Tuple[int, float]]:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("""
                SELECT EXTRACT(MONTH FROM end_date) AS month, 
                       SUM(total_price * 0.15) AS profit
                FROM CustomerReservations
                WHERE EXTRACT(YEAR FROM end_date) = {year}
                GROUP BY month
                ORDER BY month
                """).format(year=sql.Literal(year))
        rows_affected, res = conn.execute(query)
        if not rows_affected:
            return [(i, 0.0) for i in range(1,13)]
        conn.commit()
    except Exception as e:
        print(e)
        return [(i, 0.0) for i in range(1,13)]
    finally:
        conn.close()
    result = []
    missing_months = [i for i in range(1,13)]
    for tuple in res.rows:
        result.append((int(tuple[0]), float(tuple[1])))
        missing_months.remove(int(tuple[0]))
    result = result + [(i,0.0) for i in missing_months]
    result = sorted(result,key=lambda x : x[0])
    return result


def get_apartment_recommendation(customer_id: int) -> List[Tuple[Apartment, float]]:
    # TODO: implement
    pass

if __name__ == '__main__':
    drop_tables()
    create_tables()
    print(add_apartment(Apartment(1,'a','haifa','israel',100)))
    print(add_owner(Owner(10,'loay')))
    print(owner_owns_apartment(10,1))
    print(add_customer(Customer(1000,'kamil')))
    print(customer_made_reservation(1000,1,date(2024,2,20), date(2024,2,23),100))
    print(customer_reviewed_apartment(1000,1,date(2025,1,1),4,'not bad'))
    print(add_apartment(Apartment(2,'b','haifa','israel',50)))
    print(customer_made_reservation(1000,2,date(2024,2,20),date(2024,2,21),100))
    print(customer_reviewed_apartment(1000,2,date(2025,2,2),2,'very bad'))
    print(owner_owns_apartment(10,2))
    print(get_apartment_rating(2))
    print(get_owner_rating(10))
    print(get_owner_rating(11))
    print(add_customer(Customer(1001,'kamil1')))
    print(add_apartment(Apartment(3,'c','haifa','israel',100)))
    print(customer_made_reservation(1001,3,date(2024,2,20),date(2024,2,21),100.0))
    print(customer_reviewed_apartment(1001,3,date(2024,2,22),10,'very good!'))
    print(get_owner_rating(11))
    print(get_owner_rating(10))
    print(add_owner(Owner(11,'kokobalala')))
    print(owner_owns_apartment(11,3))
    print(get_owner_rating(11))
    print(customer_made_reservation(1001,3,date(2026,2,20),date(2026,2,21),100.0))
    print(get_top_customer())
    print(customer_made_reservation(1001,3,date(2027,2,20),date(2027,2,21),100.0))
    print(get_top_customer())
    print(reservations_per_owner())
    print(add_apartment(Apartment(4,'d','Tel aviv','Israel',40)))
    print(get_all_location_owners())
    print(owner_owns_apartment(11,4))
    res = get_all_location_owners()
    for owner in res:
        print(owner)
    print(best_value_for_money())
    print(customer_made_reservation(1001,3,date(2027,5,20),date(2027,5,21),10000.0))
    print(customer_made_reservation(1001,3,date(2027,5,23),date(2027,5,26),234.0))
    print(customer_made_reservation(1001,3,date(2027,6,20),date(2027,9,21),10000.0))
    print(profit_per_month(2027))
    print(customer_made_reservation(1000,3,date(2029,2,20),date(2029,2,21),100.0))
    print(customer_reviewed_apartment(1000,3,date(2030,2,22),5,'very good!'))

