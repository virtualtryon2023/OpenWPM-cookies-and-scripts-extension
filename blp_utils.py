from urllib.parse import urlparse
from publicsuffix2 import PublicSuffixList
from BlockListParser import BlockListParser


# Manual mapping created by examining the content types of responses on the
# top 1 million homepages in March 2016
content_type_map = {
    'script': lambda x: (
        'javascript' in x
        or 'ecmascript' in x
        or x.endswith('text/js')
    ),
    'image': lambda x: (
        'image' in x
        or 'img' in x
        or 'jpg' in x
        or 'jpeg' in x
        or 'gif' in x
        or 'png' in x
        or 'ico' in x
    ),
    'video': lambda x: (
        ('video' in x
        or 'movie' in x
        or 'mp4' in x
        or 'webm' in x)
        and 'flv' not in x
    ),
    'css': lambda x: 'css' in x,
    'html': lambda x: 'html' in x,
    'plain': lambda x: 'plain' in x and 'html' not in x,
    'font': lambda x: 'font' in x or 'woff' in x,
    'json': lambda x: 'json' in x,
    'xml': lambda x: 'xml' in x and 'image' not in x,
    'flash': lambda x: 'flash' in x or 'flv' in x or 'swf' in x,
    'audio': lambda x: 'audio' in x,
    'stream': lambda x: 'octet-stream' in x,
    'form': lambda x: 'form' in x,
    'binary': lambda x: 'binary' in x and 'image' not in x
}

IMAGE_TYPES = {'tif', 'tiff', 'gif', 'jpeg',
               'jpg', 'jif', 'jfif', 'jp2',
               'jpx', 'j2k', 'j2c', 'fpx',
               'pcd', 'png'}

def get_top_level_type(content_type):
    """Returns a "top level" type for a given mimetype string.
    This uses a manually compiled mapping of mime types. The top level types
    returned are approximately mapped to request context types in Firefox
    Parameters
    ----------
    content_type : str
        content type string from the http response.
    Returns
    -------
    str
        "top level" content type, e.g. 'image' or 'script'
    """
    if ';' in content_type:
        content_type = content_type.split(';')[0]
    for k,v in content_type_map.items():
        if v(content_type.lower()):
            return k
    return None

def is_passive(content_type):
    """Checks if content is likely considered passive content.
    Note that browsers block on *request* context, not response. For example,
    the request generated from a <script> element will be classified as active
    content. A custom mapping of response content types is used to determine
    the likely classification, but this will be imperfect. Passive content as
    defined here (ignoring <object> subresources):
        https://developer.mozilla.org/en-US/docs/Security/Mixed_content
    Parameters
    ----------
    content_type : str
        content type string from the http response.
    Returns
    -------
    bool
        True if the content_type indicates passive content, false otherwise.
    """
    return get_top_level_type(content_type) in ['image','audio','video']

def is_active(content_type):
    """Checks if content is likely considered active content.
    Note that browsers block on *request* context, not response. For example,
    the request generated from a <script> element will be classified as active
    content. A custom mapping of response content types is used to determine
    the likely classification, but this will be imperfect.
    Parameters
    ----------
    content_type : str
        content type string from the http response.
    Returns
    -------
    bool
        True if the content_type indicates active content, false otherwise.
    """
    return not is_passive(content_type)

def is_image(url, content_type):
    """Determine if a request url is an image.
    Preference is given to the content type, but will fall back to the
    extension of the url if necessary.
    Parameters
    ----------
    url : str
        request url
    content_type : str
        content type header of the http response to the request
    Returns
    -------
    bool
        True if the request is an image, false otherwise.
    """
    if get_top_level_type(content_type) == 'image':
        return True
    extension = urlparse(url).path.split('.')[-1]
    if extension.lower() in IMAGE_TYPES:
        return True
    return False

def is_js(url, content_type):
    """Determine if a request url is javascript.
    Preference is given to the content type, but will fall back to the
    extension of the url if necessary.
    Parameters
    ----------
    url : str
        request url
    content_type : str
        content type header of the http response to the request
    Returns
    -------
    bool
        True if the request is a JS file, false otherwise.
    """
    if get_top_level_type(content_type) == 'script':
        return True
    if urlparse(url).path.split('.')[-1].lower() == 'js':
        return True
    return False

def get_option_dict(url, top_url, is_js, is_image, public_suffix_list):
    """Build an options dict for BlockListParser
    Parameters
    ----------
    url : str
        request url to be checked by BlockListParser
    top_url : str
        url of the top-level page the request is occuring on
    is_js : bool
        indicates if this request is js
    is_image : bool
        indicates if this request is an image
    public_suffix_list : PublicSuffixList
        An instance of PublicSuffixList()
    Returns
    -------
    dict
        An "options" dictionary for use with BlockListParser
    """
    options = {}
    options["image"] = is_image
    options["script"] = is_js
    options["third-party"] = False
    options["domain"] = ""
    options["top_url"] = top_url

    top_hostname = urlparse(top_url).hostname
    top_domain = public_suffix_list.get_public_suffix(top_hostname)

    hostname = urlparse(url).hostname
    domain = public_suffix_list.get_public_suffix(hostname)
    if not top_domain == domain:
        options["third-party"] = True
    options["domain"] = top_hostname
    return options


def test_domain():
    psl = PublicSuffixList()
    url = 'https://hm.baidu.com/hm.js?dbd597c0de7cf85136bf6d9d4ca79473'
    hostname = urlparse(url).hostname
    domain = psl.get_public_suffix(hostname)
    print(domain, hostname)