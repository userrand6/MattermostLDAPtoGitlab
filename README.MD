# A Quick and dirty tool that enforces a mattermost ldap to authentik migration
### What it does:

get a list of all usernames and ids from Authentik ("id": u["pk"], "username": u["username"])

connect a to the mattermost database

for all users set authtype to 'gitlab' and authdata to the user id, eg '123'

```
            UPDATE users
               SET authservice = %s,
                   authdata = %s
             WHERE username = %s;
```



### What we did with it:

Set up new Mattermost install (docker), migrate our data and database to it

set up Gitlab authentication to our Authentik server:
https://ayedo.de/posts/mattermost-self-hosted-sso-mit-authentik/

copy example.env to .env, add our info

run this script on the new copy of the database

test if login works

 DO NOT RUN ON PRODUCTION DATABASES, if it works it is probably fine to use, but this is a just a quick and dirty script, so there is a good chance that it will break stuff.

## Known issues:

Mattermost users not known by authentik stay set as ldap user

If Mattermost displays an error similar to "This user exists for a different authentication method" on login through gitlab, you probably set the wrong userid in user.authdata
