#ruby code
require 'mysql'
require 'csv'
require 'json'

to_date = '2016-05-02'
filename = "Client-20160101-#{to_date.gsub(/\-/,"")}.csv"

qry = %{select
          u.id
          , u.name
          , u.username
          , u.email
          , u.registerdate
          , u.lastvisitdate
          , pu.params
          , pm.reference
        from fft_payplans_user pu
         inner join fft_users u
          on u.id = pu.user_id
         inner join fft_payplans_modifier pm 
          on pu.user_id = pm.user_id
        where 1=1
           and u.registerdate > '2016-01-01 00:00:00'
          and u.registerdate < ?
          and u.username not in ('Not_Registered')
}

begin
  con = Mysql.new("clientsite.com","user","pwd","db_name")
  rs = con.prepare(qry)
  rs.execute "#{to_date} 23:59:59"

rescue Mysql::Error => e
    puts e.errno
    puts e.error
ensure
    con.close if con
end

b = []

#pull data into a 2d array
rs.each do |a|
   h = JSON.parse(a[6])
   b << [a[0], a[1], a[2], h["firstname"], h["lastname"] ||= "", a[3], a[4], a[5], h["city"], h["mycountry"], h["state"], h["league"], a[7] ||= "" ]; nil
end

#sort array by discountcode and lastname
sb = b.sort_by {|e| [e[12],e[4]]}
tb = []
tb.push(["userid","name","username","firstname","lastname","email","registrationdate","lastvisit","city","country","state","league","discountcode"])
sb.each {|bb| tb.push bb}

#save to csv file
File.open(filename, 'w') do |f|
   tb.each do |bb|
      f.write(CSV.generate_line(bb))
   end
end
