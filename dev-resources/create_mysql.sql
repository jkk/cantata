create table country(
    id smallint auto_increment primary key,
    country varchar(50) not null
) engine=innodb default charset=utf8;
create table city(
    id smallint auto_increment primary key,
    city varchar(50) not null,
    country_id smallint not null
) engine=innodb default charset=utf8;   
create index idx_fk_country_id on city(country_id);              
create table address(
    id smallint auto_increment primary key,
    address varchar(50) not null,
    address2 varchar(50) default null,
    district varchar(20),
    city_id smallint not null,
    postal_code varchar(10) default null,
    phone varchar(20)
) engine=innodb default charset=utf8;           
create index idx_fk_city_id on address(city_id); 
create table category(
    id tinyint auto_increment primary key,
    name varchar(25) not null
) engine=innodb default charset=utf8;              
create table store(
    id tinyint auto_increment primary key,
    address_id smallint not null
) engine=innodb default charset=utf8; 
create index idx_fk_address_id on store(address_id);             
create table customer(
    id smallint auto_increment primary key,
    store_id tinyint not null,
    first_name varchar(45) not null,
    last_name varchar(45) not null,
    email varchar(50) default null,
    address_id smallint not null,
    active boolean default true not null,
    create_date datetime not null
) engine=innodb default charset=utf8; 
create index idx_fk_store_id on customer(store_id);              
create index idx_fk_address_id2 on customer(address_id);         
create index idx_last_name on customer(last_name);               
create table language(
    id tinyint auto_increment primary key,
    name char(20) not null
) engine=innodb default charset=utf8; 
create table film(
    id smallint auto_increment primary key,
    title varchar(255) not null,
    description text default null,
    release_year year default null,
    language_id tinyint not null,
    original_language_id tinyint default null,
    rental_duration tinyint default 3 not null,
    rental_rate decimal(4, 2) default 4.99 not null,
    length smallint default null,
    replacement_cost decimal(5, 2) default 19.99 not null,
    rating varchar(20) default 'G' not null,
    special_features varchar(255) default null
) engine=innodb default charset=utf8;            
create index idx_title on film(title);           
create index idx_fk_language_id on film(language_id);            
create index idx_fk_original_language_id on film(original_language_id);          
create table film_actor(
    actor_id smallint not null,
    film_id smallint not null
) engine=innodb default charset=utf8;
alter table film_actor add constraint film_actor_pk primary key(actor_id, film_id);          
create index idx_fk_film_id on film_actor(film_id);              
create table film_category(
    film_id smallint not null,
    category_id tinyint not null
) engine=innodb default charset=utf8;
alter table film_category add constraint film_category_pk primary key(film_id, category_id);
create table inventory(
    id mediumint auto_increment primary key,
    film_id smallint not null,
    store_id tinyint not null
) engine=innodb default charset=utf8;              
create index idx_fk_film_id2 on inventory(film_id);              
create index idx_store_id_film_id on inventory(store_id, film_id);               
create table staff(
    id tinyint auto_increment primary key,
    first_name varchar(45) not null,
    last_name varchar(45) not null,
    address_id smallint not null,
    email varchar(50) default null,
    store_id tinyint not null,
    active boolean default true not null,
    username varchar(16) not null,
    password varchar(40) default null
) engine=innodb default charset=utf8; 
create index idx_fk_store_id2 on staff(store_id);
create index idx_fk_address_id3 on staff(address_id);            
create table rental(
    id int auto_increment primary key,
    rental_date datetime not null,
    inventory_id mediumint not null,
    customer_id smallint not null,
    return_date datetime default null,
    staff_id tinyint not null
) engine=innodb default charset=utf8;         
create index idx_fk_inventory_id on rental(inventory_id);        
create index idx_fk_customer_id on rental(customer_id);          
create index idx_fk_staff_id on rental(staff_id);
create table payment(
    id int auto_increment primary key,
    customer_id smallint not null,
    staff_id tinyint not null,
    rental_id int default null,
    amount decimal(5, 2) not null,
    payment_date datetime not null
) engine=innodb default charset=utf8;            
create index idx_fk_staff_id2 on payment(staff_id);              
create index idx_fk_customer_id2 on payment(customer_id);        
create table store_manager(
    store_id tinyint not null,
    manager_id tinyint not null
) engine=innodb default charset=utf8;
alter table store_manager add constraint store_manager_pk primary key(store_id, manager_id);     
create table actor(
    id smallint auto_increment primary key,
    name varchar(255)
) engine=innodb default charset=utf8;           
alter table rental add constraint constraint_8f unique(rental_date, inventory_id, customer_id);  
alter table store add foreign key(address_id) references address(id) on update cascade;       
alter table payment add foreign key(rental_id) references rental(id) on delete set null on update cascade;    
alter table inventory add foreign key(store_id) references store(id) on update cascade;       
alter table rental add foreign key(customer_id) references customer(id) on update cascade; 
alter table customer add foreign key(address_id) references address(id) on update cascade; 
alter table city add foreign key(country_id) references country(id) on update cascade;         
alter table store_manager add foreign key(manager_id) references staff(id) on update cascade;
alter table store_manager add foreign key(store_id) references store(id) on update cascade;       
alter table rental add foreign key(inventory_id) references inventory(id) on update cascade;             
alter table staff add foreign key(store_id) references store(id) on update cascade;               
alter table film_actor add foreign key(film_id) references film(id) on update cascade;         
alter table address add foreign key(city_id) references city(id) on update cascade;               
alter table payment add foreign key(staff_id) references staff(id) on update cascade;           
alter table rental add foreign key(staff_id) references staff(id) on update cascade;             
alter table inventory add foreign key(film_id) references film(id) on update cascade;           
alter table film add foreign key(original_language_id) references language(id) on update cascade;   
alter table film_actor add foreign key(actor_id) references actor(id) on update cascade;     
alter table staff add foreign key(address_id) references address(id) on update cascade;       
alter table payment add foreign key(customer_id) references customer(id) on update cascade;               
alter table film_category add foreign key(film_id) references film(id) on update cascade;   
alter table film add foreign key(language_id) references language(id) on update cascade;     
alter table customer add foreign key(store_id) references store(id) on update cascade;         
alter table film_category add foreign key(category_id) references category(id) on update cascade;