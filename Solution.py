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
                owner_name TEXT NOT NULL,
                PRIMARY KEY(owner_id),
                CHECK(owner_id > 0)
            );
            
            DROP TABLE IF EXISTS Apartment CASCADE;
            CREATE TABLE Apartment(
                apartment_id INTEGER NOT NULL,
                address TEXT NOT NULL,
                city TEXT NOT NULL,
                country TEXT NOT NULL,
                size INTEGER NOT NULL,
                CHECK(apartment_id > 0), CHECK(size > 0),
                PRIMARY KEY(apartment_id),
                UNIQUE(address, city,country)
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
                CHECK(customer_id > 0),
                CHECK(apartment_id > 0),
                FOREIGN KEY(customer_id) REFERENCES Customer(customer_id) ON DELETE CASCADE,
                FOREIGN KEY(apartment_id) REFERENCES Apartment(apartment_id) ON DELETE CASCADE,
                UNIQUE(apartment_id, start_date)
            );

            DROP TABLE IF EXISTS CustomerReviews CASCADE;
            CREATE TABLE CustomerReviews(
                customer_id INTEGER NOT NULL,
                apartment_id INTEGER NOT NULL,
                review_date DATE NOT NULL,
                rating INTEGER NOT NULL,
                review_text TEXT NOT NULL,
                CHECK (rating BETWEEN 1 AND 10),
                CHECK(customer_id > 0),
                CHECK(apartment_id > 0),
                FOREIGN KEY(customer_id) REFERENCES Customer(customer_id) ON DELETE CASCADE,
                FOREIGN KEY(apartment_id) REFERENCES Apartment(apartment_id) ON DELETE CASCADE,
                UNIQUE(customer_id, apartment_id)
            );


            DROP TABLE IF EXISTS ApartmentOwners CASCADE;
            CREATE TABLE ApartmentOwners(
                owner_id INTEGER NOT NULL,
                apartment_id INTEGER NOT NULL,
                PRIMARY KEY(apartment_id),
                FOREIGN KEY(owner_id) REFERENCES Owner(owner_id) ON DELETE CASCADE,
                FOREIGN KEY(apartment_id) REFERENCES Apartment(apartment_id) ON DELETE CASCADE
            );
            
            CREATE VIEW ApartmentOwnersFullData AS
            SELECT A.owner_id, O.owner_name AS owner_name, B.*
            FROM ApartmentOwners A
            JOIN Owner O ON A.owner_id = O.owner_id
            JOIN Apartment B ON A.apartment_id = B.apartment_id;
            
            CREATE VIEW ApartmentReviewsFullData AS
            SELECT owner_id, C.apartment_id AS apartment_id, owner_name, customer_id, review_date, rating, review_text
            FROM ApartmentOwnersFullData A RIGHT OUTER JOIN CustomerReviews C ON (A.apartment_id = C.apartment_id);
            
            CREATE VIEW ApartmentAvgRating AS
            SELECT A.owner_id AS owner_id, A.apartment_id AS apartment_id, COALESCE(AVG(rating), 0) AS avg_rating
            FROM ApartmentOwners A LEFT JOIN 
            ApartmentReviewsFullData B ON (A.apartment_id = B.apartment_id)
            GROUP BY A.apartment_id, A.owner_id;
                
            CREATE VIEW OwnerAvgRating AS
            SELECT A.owner_id, COALESCE(AVG(AR.avg_rating), 0) AS avg_rating
            FROM Owner A
            LEFT JOIN ApartmentAvgRating AR ON A.owner_id = AR.owner_id
            GROUP BY A.owner_id;


            CREATE VIEW OwnerCustomerReservations AS
            SELECT owner_id, owner_name, customer_id, A.apartment_id AS apartment_id 
            FROM ApartmentOwnersFullData A RIGHT OUTER JOIN CustomerReservations C ON (A.apartment_id=C.apartment_id);
            
            CREATE VIEW OwnerReservations AS
            SELECT O.owner_name, COUNT(OCR.owner_id) AS reservations
            FROM Owner O
            LEFT JOIN OwnerCustomerReservations OCR ON O.owner_id = OCR.owner_id
            GROUP BY O.owner_name, O.owner_id;
                        
            CREATE VIEW ApartmentPriceRatingAVG AS
            SELECT B.apartment_id AS apartment_id, address, city, country, size ,total_price / (end_date - start_date) AS price_per_night, COALESCE(rating, 0) AS rating
            FROM CustomerReservations C FULL OUTER JOIN ApartmentReviewsFullData A ON (C.apartment_id=A.apartment_id)
            JOIN Apartment B ON (C.apartment_id = B.apartment_id);

            CREATE VIEW CustomerReviewsProd AS
            SELECT A.customer_id AS customer_a_id,
                   B.customer_id AS customer_b_id,
                   A.apartment_id AS apartment_id,
                   A.rating AS customer_a_rating,
                   B.rating AS customer_b_rating
            FROM CustomerReviews A, CustomerReviews B
            WHERE A.customer_id != B.customer_id AND A.apartment_id = B.apartment_id;
            
            CREATE VIEW CustomerRatingsAvgRatio AS
            SELECT customer_a_id, customer_b_id, AVG(customer_a_rating*1.0/customer_b_rating) AS avg_ratio
            FROM CustomerReviewsProd A
            GROUP BY customer_a_id, customer_b_id;
            
            CREATE VIEW UnreviewedApartments AS
            SELECT C.customer_id AS customer_id, A.apartment_id AS unreviewed_apartment_id
            FROM CustomerReviews C , Apartment A
            WHERE NOT EXISTS (
                SELECT 1
                FROM CustomerReviews D
                WHERE D.customer_id = C.customer_id AND A.apartment_id = D.apartment_id
                )
            GROUP BY customer_id, unreviewed_apartment_id;
            
            CREATE VIEW CustomersUnreviewedApartmentsAvgRatio AS
            SELECT customer_a_id, customer_b_id, avg_ratio, unreviewed_apartment_id
            FROM CustomerRatingsAvgRatio C JOIN UnreviewedApartments U ON (C.customer_a_id = U.customer_id);
            
            CREATE VIEW CustomersUnreviewedApartmentsFilter AS
            SELECT customer_a_id AS customer_id, unreviewed_apartment_id, AVG(GREATEST(LEAST(rating*avg_ratio, 10), 1)) AS expected_rating
            FROM CustomersUnreviewedApartmentsAvgRatio C JOIN CustomerReviews A ON (C.customer_b_id = A.customer_id AND C.unreviewed_apartment_id = A.apartment_id)
            GROUP BY customer_a_id, unreviewed_apartment_id;
            
            CREATE VIEW TopCustomer AS
            SELECT A.customer_id AS customer_id, customer_name
                    FROM Customer A JOIN CustomerReservations B ON (A.customer_id = B.customer_id)
                    GROUP BY A.customer_id, customer_name
                    ORDER BY COUNT(*) DESC, customer_id ASC
                    LIMIT 1;
            
            CREATE VIEW total_distinct_cities AS
            SELECT COUNT(DISTINCT (city, country)) AS total_cities
            FROM Apartment;

            CREATE VIEW distinct_cities_per_owner AS
            SELECT owner_id, owner_name, COUNT(DISTINCT (Apartment.city, Apartment.country)) AS cities_per_owner
            FROM ApartmentOwnersFullData
            JOIN Apartment ON ApartmentOwnersFullData.apartment_id = Apartment.apartment_id
            GROUP BY owner_id, owner_name;

            CREATE VIEW CustomerUnreviewedApartmentsFullData AS
            SELECT *
            FROM CustomersUnreviewedApartmentsFilter C JOIN Apartment A ON (C.unreviewed_apartment_id = A.apartment_id);
            
            COMMIT;
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
        conn.execute("""
                     Begin;
                     DELETE FROM Owner;
                     DELETE FROM Apartment;
                     DELETE FROM Customer;
                     DELETE FROM CustomerReservations;
                     DELETE FROM CustomerReviews;
                     DELETE FROM ApartmentOwners;
                     COMMIT;
                     """)
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
        conn.execute("""
                     Begin;
                     DROP TABLE IF EXISTS Owner CASCADE;
                     DROP TABLE IF EXISTS Apartment CASCADE;
                     DROP TABLE IF EXISTS Customer CASCADE;
                     DROP TABLE IF EXISTS CustomerReservations CASCADE;
                     DROP TABLE IF EXISTS CustomerReviews CASCADE;
                     DROP TABLE IF EXISTS ApartmentOwners CASCADE;
                     DROP VIEW IF EXISTS ApartmentOwnersFullData CASCADE;
                     DROP VIEW IF EXISTS ApartmentReviewsFullData CASCADE;
                     DROP VIEW IF EXISTS ApartmentAvgRating CASCADE;
                     DROP VIEW IF EXISTS OwnerAvgRating CASCADE;
                     DROP VIEW IF EXISTS OwnerCustomerReservations CASCADE;
                     DROP VIEW IF EXISTS OwnerReservations CASCADE;
                     DROP VIEW IF EXISTS ApartmentPriceRatingAVG CASCADE;
                     DROP VIEW IF EXISTS CustomerReviewsProd CASCADE;
                     DROP VIEW IF EXISTS CustomerRatingsAvgRatio CASCADE;
                     DROP VIEW IF EXISTS UnreviewedApartments CASCADE;
                     DROP VIEW IF EXISTS CustomersUnreviewedApartmentsAvgRatio CASCADE;
                     DROP VIEW IF EXISTS CustomersUnreviewedApartmentsFilter CASCADE;
                     DROP VIEW IF EXISTS TopCustomer CASCADE;
                     DROP VIEW IF EXISTS CustomerUnreviewedApartmentsFullData CASCADE;
                     DROP VIEW IF EXISTS distinct_cities_per_owner CASCADE;
                     DROP VIEW IF EXISTS total_distinct_cities CASCADE;
                     COMMIT;
                     """)
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
        owner_name = owner.get_owner_name()
        query = sql.SQL("INSERT INTO Owner(owner_id,owner_name) values({owner_id} , {owner_name});").format(owner_id=sql.Literal(owner_id), owner_name = sql.Literal(owner_name))
        conn.execute(query)
        conn.commit()
    except (DatabaseException.NOT_NULL_VIOLATION,DatabaseException.CHECK_VIOLATION) as e:
        # print(e)
        return ReturnValue.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION as e:
        # print(e)
        return ReturnValue.ALREADY_EXISTS
    except Exception as e:
        # print(e)
        return ReturnValue.ERROR
    finally:
        conn.close()

    return ReturnValue.OK

def res_to_owner(res: Connector.ResultSet) -> Owner:
    return Owner(res[0]['owner_id'], res[0]['owner_name'])

def get_owner(owner_id: int) -> Owner:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT * " +
                        "FROM Owner " +
                        "WHERE owner_id = {owner_id};").format(owner_id = sql.Literal(owner_id))
        rows_affected, res = conn.execute(query)
        # If the result of the query returned empty table it means that an owner with the requested apartment_id does not exist
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
    if not owner_id or owner_id <= 0:
        return ReturnValue.BAD_PARAMS
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
        apartment_id = apartment.get_id()
        address = apartment.get_address()
        city = apartment.get_city()
        country = apartment.get_country()
        size = apartment.get_size()
        query = sql.SQL("INSERT INTO Apartment(apartment_id, address, city, country, size) values({apartment_id}, " +
                        "{address}, {city}, {country},{size});") \
            .format(apartment_id=sql.Literal(apartment_id),
                    address=sql.Literal(address),
                    city=sql.Literal(city),
                    country=sql.Literal(country),
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
    return Apartment(res[0]['apartment_id'], res[0]['address'], res[0]['city'], res[0]['country'], res[0]['size'])

def get_apartment(apartment_id: int) -> Apartment:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT * " +
                        "FROM Apartment " +
                        "WHERE apartment_id = {apartment_id};").format(apartment_id = sql.Literal(apartment_id))
        rows_affected, res = conn.execute(query)
        # If the result of the query returned empty table it means that an apartment with the requested apartment_id does not exist
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
    if not apartment_id or apartment_id <= 0:
        return ReturnValue.BAD_PARAMS
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("DELETE FROM Apartment WHERE apartment_id = {apartment_id};").format(apartment_id=sql.Literal(apartment_id))
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
        # If the result of the query returned empty table it means that a customer with the requested apartment_id does not exist
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
    if not customer_id or customer_id <= 0:
        return ReturnValue.BAD_PARAMS
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
                          ({end_date} > start_date AND {end_date} <= end_date AND {start_date} <= start_date) OR
                          ({start_date} >= start_date AND {start_date} < end_date AND {end_date} >= end_date))
                
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
        # print(e)
        return ReturnValue.ERROR

    finally:
        conn.close()

    return ReturnValue.OK


def customer_cancelled_reservation(customer_id: int, apartment_id: int, start_date: date) -> ReturnValue:
    if not customer_id or customer_id <= 0 or not apartment_id or apartment_id <= 0:
        return ReturnValue.BAD_PARAMS
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("""
                            DELETE FROM CustomerReservations
                            WHERE customer_id = {customer_id} AND apartment_id = {apartment_id} AND start_date = {start_date};
                            """).format(customer_id=sql.Literal(customer_id),
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
                AND ( {review_date} >= end_date) AND ({customer_id} = customer_id)
            );
        """).format(customer_id=sql.Literal(customer_id),
                    apartment_id=sql.Literal(apartment_id),
                    review_date=sql.Literal(review_date),
                    rating=sql.Literal(rating),
                    review_text=sql.Literal(review_text))
        rows_affected, _ = conn.execute(query)
        # Check if the apartment isn't available at the specified date
        if not rows_affected:
            # Check if need to return bad params before this
            if not customer_id or customer_id <= 0 or not apartment_id or apartment_id <= 0 or rating < 1 or rating > 10:
                return ReturnValue.BAD_PARAMS
            return ReturnValue.NOT_EXISTS
        conn.commit()
    except (DatabaseException.NOT_NULL_VIOLATION, DatabaseException.CHECK_VIOLATION) as e:
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
        if not rows_affected:            # Check if need to return bad params before this
            if not customer_id or customer_id <= 0 or not apartment_id or apartment_id <= 0 or new_rating < 1 or new_rating > 10:
                return ReturnValue.BAD_PARAMS
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
    if not owner_id or not apartment_id or owner_id <= 0 or apartment_id <= 0:
        return ReturnValue.BAD_PARAMS
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


def owner_drops_apartment(owner_id: int, apartment_id: int) -> ReturnValue:
    if not owner_id or not apartment_id or owner_id <= 0 or apartment_id <= 0:
        return ReturnValue.BAD_PARAMS
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
                    SELECT owner_id, owner_name
                    FROM ApartmentOwnersFullData
                    WHERE apartment_id = {apartment_id};
                """).format(apartment_id = sql.Literal(apartment_id))

        rows_affected, res = conn.execute(query)
        # If the result of the query returned empty table it means that an apartment with the requested apartment_id does not exist
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
                    SELECT apartment_id, address, city, country, size
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
                    SELECT *
                    FROM TopCustomer;
                """).format()

        rows_affected, res = conn.execute(query)
        if not rows_affected:
            return Customer.bad_customer()
        conn.commit()
    except Exception as e:
        return Customer.bad_customer()
    finally:
        conn.close()

    return res_to_customer(res)


def reservations_per_owner() -> List[Tuple[str, int]]:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("""
                    SELECT *
                    FROM OwnerReservations;
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
def get_all_location_owners() -> List[Owner]:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("""
                    SELECT owner_id, owner_name
                    FROM distinct_cities_per_owner
                    WHERE cities_per_owner = (SELECT total_cities FROM total_distinct_cities);
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
        all_location_owners.append(Owner(row['owner_id'], row['owner_name']))
    return all_location_owners


def best_value_for_money() -> Apartment:
    conn = None
    try:
        conn = Connector.DBConnector()
        # We can use the aggregation functions here as the apartment apartment_id has one address and one city etc...
        query = sql.SQL("""
                    SELECT 
                        apartment_id AS apartment_id, 
                        MAX(address) AS address, 
                        MAX(city) AS city, 
                        MAX(country) AS country, 
                        MAX(size) AS size, 
                        AVG(rating) / AVG(price_per_night) AS value_for_money
                    FROM 
                        ApartmentPriceRatingAvg
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
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("""
                SELECT *
                FROM CustomerUnreviewedApartmentsFullData
                WHERE customer_id = {customer_id};
                """).format(customer_id=sql.Literal(customer_id))
        rows_affected, res = conn.execute(query)
        if not rows_affected:
            return []
        conn.commit()
    except Exception as e:
        print(e)
        return []
    finally:
        conn.close()
    apartment_recommendations = []
    for tuple in res:
        apartment_recommendations.append((Apartment(tuple['unreviewed_apartment_id'], tuple['address'], tuple['city'], tuple['country'], tuple['size']), float(tuple['expected_rating'])))
    return apartment_recommendations

