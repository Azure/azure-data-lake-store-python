import time, requests
from .retry import retry_decorator_for_auth

try:
    import msal
    _adls_sdk_is_using_msal = True
except ImportError:
    _adls_sdk_is_using_msal = False
    try: 
        import adal
        _adls_sdk_is_using_adal = True
    except ImportError as e:
        raise e
    
def DataLakeCredentialChooser(token):
    if _adls_sdk_is_using_msal:
        return DataLakeCredentialMSAL(token)
    elif _adls_sdk_is_using_adal:
        return DataLakeCredentialADAL(token)
    else:
        raise NotImplementedError
    
def AuthChooser(tenant_id, username,
         password, client_id,
         client_secret, resource,
         require_2fa, authority, retry_policy, **kwargs):
    if _adls_sdk_is_using_msal:
        return auth_msal(tenant_id, username,
         password, client_id,
         client_secret, resource,
         require_2fa, authority, retry_policy, **kwargs)
    elif _adls_sdk_is_using_adal:
        return auth_adal(tenant_id, username,
         password, client_id,
         client_secret, resource,
         require_2fa, authority, retry_policy, **kwargs)
    else:
        raise NotImplementedError
class DataLakeCredentialMSAL:
    # Be careful modifying this. DataLakeCredential with DataLakeCredential is a general class in azure(used in azure cli), and we have to maintain parity.
    def __init__(self, token):
        self.token = token

    def signed_session(self):
        # type: () -> requests.Session
        """Create requests session with any required auth headers applied.

        :rtype: requests.Session
        """
        session = requests.Session()
        scheme, token = self.token['token_type'], self.token['access_token']
        header = "{} {}".format(scheme, token)
        session.headers['Authorization'] = header
        return session

    def refresh_token(self, authority=None):
        """ Refresh an expired authorization token

        Parameters
        ----------
        authority: string
            The full URI of the authentication authority to authenticate against (such as https://login.microsoftonline.com/)
        """
        if self.token.get('refresh', False) is False and (not self.token.get('secret') or not self.token.get('client')):
            raise ValueError("Token cannot be auto-refreshed.")

        if not authority:
            authority = 'https://login.microsoftonline.com/'

        
        scope = ["https://datalake.azure.net/.default"]
        contextPub = msal.PublicClientApplication(client_id=client_id, authority=authority+tenant_id)

        if self.token.get('secret') and self.token.get('client'):
            client_id = self.token['client']
            tenant_id  = self.token['tenant']
            client_secret = self.token['secret']
            contextClient = msal.ConfidentialClientApplication(client_id=client_id, authority=authority+tenant_id, client_credential=client_secret)
            out = contextClient.acquire_token_for_client(scopes=["https://datalake.azure.net/.default"])
            out.update({'secret': self.token['secret']})
        else:
           
            out = contextPub.client.obtain_token_by_refresh_token(self.token['refresh'], scopes=scope, )

        # common items to update
        out.update({'access': out['accessToken'],
                    'time': time.time(), 'tenant': self.token['tenant'],
                    'resource': self.token['resource'], 'client': self.token['client']})

        self.token = out

class DataLakeCredentialADAL:
    # Be careful modifying this. DataLakeCredential is a general class in azure, and we have to maintain parity.
    def __init__(self, token):
        self.token = token

    def signed_session(self):
        # type: () -> requests.Session
        """Create requests session with any required auth headers applied.
        :rtype: requests.Session
        """
        session = requests.Session()
        if time.time() - self.token['time'] > self.token['expiresIn'] - 100:
            self.refresh_token()

        scheme, token = self.token['tokenType'], self.token['access']
        header = "{} {}".format(scheme, token)
        session.headers['Authorization'] = header
        return session

    def refresh_token(self, authority=None):
        """ Refresh an expired authorization token
        Parameters
        ----------
        authority: string
            The full URI of the authentication authority to authenticate against (such as https://login.microsoftonline.com/)
        """
        if self.token.get('refresh', False) is False and (not self.token.get('secret') or not self.token.get('client')):
            raise ValueError("Token cannot be auto-refreshed.")

        if not authority:
            authority = 'https://login.microsoftonline.com/'

        context = adal.AuthenticationContext(authority +
                                             self.token['tenant'])

        if self.token.get('secret') and self.token.get('client'):
            out = context.acquire_token_with_client_credentials(self.token['resource'],
                                                                self.token['client'],
                                                                self.token['secret'])
            out.update({'secret': self.token['secret']})
        else:
            out = context.acquire_token_with_refresh_token(self.token['refresh'],
                                                           client_id=self.token['client'],
                                                           resource=self.token['resource'])
        # common items to update
        out.update({'access': out['accessToken'],
                    'time': time.time(), 'tenant': self.token['tenant'],
                    'resource': self.token['resource'], 'client': self.token['client']})

        self.token = out


# TODO: a breaking change should be made to add a new parameter specific for service_principal_app_id
# instead of overloading client_id, which is also used by other login methods to indicate what application
# is requesting the authentication (for example, in an interactive prompt).
def auth_msal(tenant_id, username,
         password, client_id,
         client_secret, resource,
         require_2fa, authority, retry_policy, **kwargs):
    """ User/password authentication

    Parameters
    ----------

    tenant_id: str
        associated with the user's subscription, or "common"
    username: str
        active directory user
    password: str
        sign-in password
    client_id: str
        the service principal client
    client_secret: str
        the secret associated with the client_id
    resource: str
        resource for auth (e.g., https://datalake.azure.net/)
    require_2fa: bool
        indicates this authentication attempt requires two-factor authentication
    authority: string
        The full URI of the authentication authority to authenticate against (such as https://login.microsoftonline.com/)
    kwargs: key/values
        Other parameters, for future use

    Returns
    -------
    :type DataLakeCredential :mod: `A DataLakeCredential object`
    """

    if not authority:
        authority = 'https://login.microsoftonline.com/'



    contextPub = msal.PublicClientApplication(client_id=client_id, authority=authority+tenant_id)
    if tenant_id is None or client_id is None:
        raise ValueError("tenant_id and client_id must be supplied for authentication")


    contextClient = msal.ConfidentialClientApplication(client_id=client_id, authority=authority+tenant_id, client_credential=client_secret)
    # You can explicitly authenticate with 2fa, or pass in nothing to the auth call
    # and the user will be prompted to login interactively through a browser.
    scope = ["https://datalake.azure.net/.default"]
    @retry_decorator_for_auth(retry_policy=retry_policy)
    def get_token_internal():
        # Internal function used so as to use retry decorator
        if require_2fa or (username is None and password is None and client_secret is None):
            flow = contextPub.initiate_device_flow(scopes=scope)
            print(flow['message'])
            out = contextPub.acquire_token_by_device_flow(flow)
        elif username and password:
            out = contextPub.acquire_token_by_username_password(username=username, password=password, scopes=scope)
        elif client_id and client_secret:
            out = contextClient.acquire_token_for_client(scopes=["https://datalake.azure.net/.default"])
            # for service principal, we store the secret in the credential object for use when refreshing.
            out.update({'secret': client_secret})
        else:
            raise ValueError("No authentication method found for credentials")
        return out
    out = get_token_internal()

    out.update({'access': out['access_token'], 'resource': resource,
                'refresh': out.get('refresh_token', False),
                'time': time.time(), 'tenant': tenant_id, 'client': client_id})

    return DataLakeCredentialMSAL(out)


# TODO: a breaking change should be made to add a new parameter specific for service_principal_app_id
# instead of overloading client_id, which is also used by other login methods to indicate what application
# is requesting the authentication (for example, in an interactive prompt).
def auth_adal(tenant_id, username,
         password, client_id,
         client_secret, resource,
         require_2fa, authority, retry_policy, **kwargs):
    """ User/password authentication
    Parameters
    ----------
    tenant_id: str
        associated with the user's subscription, or "common"
    username: str
        active directory user
    password: str
        sign-in password
    client_id: str
        the service principal client
    client_secret: str
        the secret associated with the client_id
    resource: str
        resource for auth (e.g., https://datalake.azure.net/)
    require_2fa: bool
        indicates this authentication attempt requires two-factor authentication
    authority: string
        The full URI of the authentication authority to authenticate against (such as https://login.microsoftonline.com/)
    kwargs: key/values
        Other parameters, for future use
    Returns
    -------
    :type DataLakeCredential :mod: `A DataLakeCredential object`
    """
    context = adal.AuthenticationContext(authority +
                                         tenant_id)

    if tenant_id is None or client_id is None:
        raise ValueError("tenant_id and client_id must be supplied for authentication")

    # You can explicitly authenticate with 2fa, or pass in nothing to the auth call
    # and the user will be prompted to login interactively through a browser.

    @retry_decorator_for_auth(retry_policy=retry_policy)
    def get_token_internal():
        # Internal function used so as to use retry decorator
        if require_2fa or (username is None and password is None and client_secret is None):
            code = context.acquire_user_code(resource, client_id)
            print(code['message'])
            out = context.acquire_token_with_device_code(resource, code, client_id)
        elif username and password:
            out = context.acquire_token_with_username_password(resource, username,
                                                               password, client_id)
        elif client_id and client_secret:
            out = context.acquire_token_with_client_credentials(resource, client_id,
                                                                client_secret)
            # for service principal, we store the secret in the credential object for use when refreshing.
            out.update({'secret': client_secret})
        else:
            raise ValueError("No authentication method found for credentials")
        return out
    out = get_token_internal()

    out.update({'access': out['accessToken'], 'resource': resource,
                'refresh': out.get('refreshToken', False),
                'time': time.time(), 'tenant': tenant_id, 'client': client_id})

    return DataLakeCredentialADAL(out)