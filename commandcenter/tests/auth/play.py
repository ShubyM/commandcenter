

from bonsai import LDAPClient




client: LDAPClient = LDAPClient(
    url="ldap://127.0.0.1:3004",

)



print(client.get_rootDSE())


