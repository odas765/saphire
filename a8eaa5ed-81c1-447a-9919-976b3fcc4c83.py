import logging
import re
import shutil
import ffmpeg
from datetime import datetime, timedelta
import os
import sqlite3

from utils.models import *
from utils.utils import create_temp_filename
from .beatport_api import BeatportApi

module_information = ModuleInformation(
    service_name='Beatport',
    module_supported_modes=ModuleModes.download | ModuleModes.covers,
    session_settings={
        'username': '',
        'password': '',
        'debug': bool
    },
    session_storage_variables=['access_token', 'refresh_token', 'expires'],
    netlocation_constant='beatport',
    url_decoding=ManualEnum.manual,
    test_url='https://www.beatport.com/track/darkside/10844269'
)

class ModuleInterface:
    # noinspection PyTypeChecker
    def __init__(self, module_controller: ModuleController):
        self.exception = module_controller.module_error
        self.disable_subscription_check = module_controller.orpheus_options.disable_subscription_check
        self.oprinter = module_controller.printer_controller
        self.print = module_controller.printer_controller.oprint
        self.module_controller = module_controller
        self.cover_size = module_controller.orpheus_options.default_cover_options.resolution
        
        # Initialize API with debug setting from module settings
        self.session = BeatportApi()
        self.session.debug_enabled = module_controller.module_settings.get('debug', False)

        # Quality tier mapping
        self.quality_parse = {
            QualityEnum.MINIMUM: "low",      # 128k AAC
            QualityEnum.LOW: "low",          # 128k AAC
            QualityEnum.MEDIUM: "medium",    # 128k AAC
            QualityEnum.HIGH: "high",        # 256k AAC
            QualityEnum.LOSSLESS: "flac",    # FLAC
            QualityEnum.HIFI: "flac"         # FLAC
        }

        # Login using credentials from settings
        if not self.disable_subscription_check:
            self.login(
                module_controller.module_settings['username'],
                module_controller.module_settings['password']
            )

    def login(self, email: str, password: str):
        """Login and validate account"""
        # Check if we already have valid tokens by trying subscription check first
        try:
            subscription = self.session.get_subscription()
            
            # Check if we got a valid user response
            if subscription.get('user_id') and subscription.get('subscription'):
                # If we get here with valid user data, validate subscription
                scopes = subscription.get('scope', '').split()
                if 'user:dj' not in scopes:
                    raise self.exception('Account does not have DJ/streaming permissions')
                    
                # Check subscription type
                sub_type = subscription.get('subscription')
                if not sub_type:
                    raise self.exception('No active subscription found')
                    
                # Verify it's a LINK or LINK PRO subscription
                if sub_type not in ['bp_link', 'bp_link_pro']:
                    raise self.exception('Account does not have a LINK or LINK PRO subscription')
                    
                # Check features
                features = subscription.get('feature', [])
                required_features = [
                    'feature:fulltrackplayback',
                    'feature:cdnfulfillment',
                    'feature:cdnfulfillment-link'
                ]
                
                missing_features = [f for f in required_features if f not in features]
                if missing_features:
                    raise self.exception(f'Account missing required features: {", ".join(missing_features)}')
                    
                return  # Already logged in with valid tokens
            
        except (ValueError, ConnectionError):
            # If subscription check fails, we need to login
            pass
            
        # Perform fresh login
        login_data = self.session.auth(email, password)
        if login_data.get('error_description'):
            raise self.exception(login_data.get('error_description'))

        # Validate subscription using introspect endpoint
        subscription = self.session.get_subscription()
        
        # Check scopes in the introspection response
        scopes = subscription.get('scope', '').split()
        if 'user:dj' not in scopes:
            raise self.exception('Account does not have DJ/streaming permissions')
            
        # Check subscription type
        sub_type = subscription.get('subscription')
        if not sub_type:
            raise self.exception('No active subscription found')
            
        # Verify it's a LINK or LINK PRO subscription
        if sub_type not in ['bp_link', 'bp_link_pro']:
            raise self.exception('Account does not have a LINK or LINK PRO subscription')
            
        # Check features
        features = subscription.get('feature', [])
        required_features = [
            'feature:fulltrackplayback',
            'feature:cdnfulfillment',
            'feature:cdnfulfillment-link'
        ]
        
        missing_features = [f for f in required_features if f not in features]
        if missing_features:
            raise self.exception(f'Account missing required features: {", ".join(missing_features)}')

    @staticmethod
    def custom_url_parse(link: str):
        # Add handling for generic chart URLs
        if '/top-100' in link or '/hype-100' in link:
            return MediaIdentification(
                media_type=DownloadTypeEnum.playlist,
                media_id=link,  # Pass the full URL as the ID
                extra_kwargs={'is_chart': True, 'is_url_chart': True}
            )
        
        # First check if it's a library playlist URL
        library_match = re.search(r"https?://(www.)?beatport.com/library/playlists/(\d+)", link)
        if library_match:
            playlist_id = library_match.group(2)
            return MediaIdentification(
                media_type=DownloadTypeEnum.playlist,
                media_id=playlist_id,
                extra_kwargs={'is_library': True}  # Flag to use my/playlists endpoint
            )

        # Check if it's a genre chart URL
        genre_chart_match = re.search(r"/genre/[^/]+/(\d+)/hype-(\d+)", link)
        if genre_chart_match:
            genre_id = genre_chart_match.group(1)
            chart_type = genre_chart_match.group(2)
            chart_id = f"genre-{genre_id}-hype-{chart_type}"
            return MediaIdentification(
                media_type=DownloadTypeEnum.playlist,
                media_id=chart_id,
                extra_kwargs={'is_chart': True}
            )

        # Handle regular URLs
        match = re.search(r"https?://(www.)?beatport.com/(?:[a-z]{2}/)?"
                          r"(?P<type>track|release|artist|playlists|chart)/.+?/(?P<id>\d+)", link)

        if not match:
            raise ValueError("Invalid URL format")

        media_types = {
            'track': DownloadTypeEnum.track,
            'release': DownloadTypeEnum.album,
            'artist': DownloadTypeEnum.artist,
            'playlists': DownloadTypeEnum.playlist,
            'chart': DownloadTypeEnum.playlist
        }

        return MediaIdentification(
            media_type=media_types[match.group('type')],
            media_id=match.group('id'),
            extra_kwargs={'is_chart': match.group('type') == 'chart'}
        )

    def get_playlist_info(self, playlist_id: str, is_chart: bool = False, is_library: bool = False, **kwargs) -> PlaylistInfo:
        """Get playlist info with support for URL-based charts"""
        # Handle URL-based charts
        if is_chart and isinstance(playlist_id, str) and playlist_id.startswith('http'):
            try:
                self.print("Starting chart scraping...")
                
                # Check if it's a genre top 100 chart
                if "/genre/" in playlist_id and ("/top-100" in playlist_id or "/hype-100" in playlist_id):
                    from selenium import webdriver
                    from selenium.webdriver.common.by import By
                    from selenium.webdriver.support.ui import WebDriverWait
                    from selenium.webdriver.support import expected_conditions as EC
                    import time
                    
                    self.print("Initializing Chrome driver for genre chart...")
                    chrome_options = webdriver.ChromeOptions()
                    chrome_options.add_argument('--headless')
                    chrome_options.add_argument('--disable-gpu')
                    chrome_options.add_argument('--no-sandbox')
                    chrome_options.add_argument('--disable-dev-shm-usage')
                    chrome_options.add_argument("--window-size=1920,1080")
                    
                    driver = None
                    try:
                        driver = webdriver.Chrome(options=chrome_options)
                        self.print(f"Navigating to genre chart URL: {playlist_id}")
                        driver.get(playlist_id)
                        time.sleep(5)
                        
                        # Wait for any track link to appear
                        self.print("Looking for tracks...")
                        WebDriverWait(driver, 20).until(
                            EC.presence_of_element_located((By.CSS_SELECTOR, "a[href*='/track/']"))
                        )
                        
                        # Get all track IDs
                        track_links = driver.find_elements(By.CSS_SELECTOR, "a[href*='/track/']")
                        tracks = []
                        seen_ids = set()  # To avoid duplicates since each track has multiple links
                        
                        for link in track_links:
                            try:
                                track_url = link.get_attribute("href")
                                track_id = track_url.split("/")[-1]
                                if track_id not in seen_ids and track_id.isdigit():
                                    tracks.append(track_id)
                                    seen_ids.add(track_id)
                                
                            except Exception as e:
                                self.print(f"Error processing track: {str(e)}")
                                continue
                                
                        if not tracks:
                            if driver:
                                driver.quit()
                            raise self.exception("No tracks found in chart")
                            
                        self.print(f"\nFound {len(tracks)} tracks")
                        
                        # Get the actual genre name from the page
                        self.print("Getting genre name...")
                        try:
                            genre_elem = WebDriverWait(driver, 10).until(
                                EC.presence_of_element_located((By.CSS_SELECTOR, "h1.pre-text"))
                            )
                            genre = genre_elem.text.strip()
                        except:
                            # Fallback to URL parsing if element not found
                            genre = playlist_id.split("/genre/")[1].split("/")[0].replace("-", " ").title()
                            
                        # Now we can safely quit the driver
                        if driver:
                            driver.quit()
                        
                        # Determine chart type
                        chart_type = "Top 100" if "/top-100" in playlist_id else "Hype 100"
                        
                        # Format the chart name with current date
                        chart_name = f"{genre} - {chart_type} ({datetime.now().strftime('%d-%m-%Y')})"
                        
                        return PlaylistInfo(
                            name=chart_name,
                            creator="Beatport",
                            release_year=datetime.now().year,
                            duration=0,  # We'll calculate this when downloading
                            tracks=tracks,
                            cover_url=None,
                            track_extra_kwargs={'data': {}}
                        )
                        
                    except Exception as e:
                        if driver:
                            driver.quit()
                        raise e
                
                # Check if it's a releases chart
                if "/top-100-releases" in playlist_id:
                    return self._get_releases_chart_info(playlist_id)
                
                # Rest of the existing track chart scraping code...
                
            except Exception as e:
                raise self.exception(f"Failed to process chart: {str(e)}")
        
        # Handle regular charts and playlists
        if is_chart:
            playlist_data = self.session.get_chart(playlist_id)
            playlist_tracks_data = self.session.get_chart_tracks(playlist_id)
        elif is_library:
            playlist_data = self.session.get_library_playlist(playlist_id)
            playlist_tracks_data = self.session.get_library_playlist_tracks(playlist_id)
        else:
            playlist_data = self.session.get_playlist(playlist_id)
            playlist_tracks_data = self.session.get_playlist_tracks(playlist_id)
        
        cache = {'data': {}}

        # now fetch all the found total_items
        if is_chart:
            playlist_tracks = playlist_tracks_data.get('results')
        else:
            playlist_tracks = [t.get('track') for t in playlist_tracks_data.get('results')]

        total_tracks = playlist_tracks_data.get('count')
        for page in range(2, (total_tracks - 1) // 100 + 2):
            print(f'Fetching {len(playlist_tracks)}/{total_tracks}', end='\r')
            # get the DJ chart or user playlist
            if is_chart:
                playlist_tracks += self.session.get_chart_tracks(playlist_id, page=page).get('results')
            else:
                # unfold the track element
                playlist_tracks += [t.get('track')
                                    for t in self.session.get_playlist_tracks(playlist_id, page=page).get('results')]

        for i, track in enumerate(playlist_tracks):
            # add the track numbers
            track['track_number'] = i + 1
            track['total_tracks'] = total_tracks
            # add the modified track to the track_extra_kwargs
            cache['data'][track.get('id')] = track

        creator = 'User'
        if is_chart:
            creator = playlist_data.get('person').get('owner_name') if playlist_data.get('person') else 'Beatport'
            release_year = playlist_data.get('change_date')[:4] if playlist_data.get('change_date') else None
            cover_url = playlist_data.get('image').get('dynamic_uri')
        else:
            release_year = playlist_data.get('updated_date')[:4] if playlist_data.get('updated_date') else None
            # always get the first image of the four total images, why is there no dynamic_uri available? Annoying
            cover_url = playlist_data.get('release_images')[0]

        return PlaylistInfo(
            name=playlist_data.get('name'),
            creator=creator,
            release_year=release_year,
            duration=sum([t.get('length_ms', 0) // 1000 for t in playlist_tracks]),
            tracks=[t.get('id') for t in playlist_tracks],
            cover_url=self._generate_artwork_url(cover_url, self.cover_size),
            track_extra_kwargs=cache
        )

    @staticmethod
    def _generate_artwork_url(cover_url: str, size: int, max_size: int = 1400):
        # if more than max_size are requested, cap the size at max_size
        if size > max_size:
            size = max_size

        # check if it's a dynamic_uri, if not make it one
        res_pattern = re.compile(r'\d{3,4}x\d{3,4}')
        match = re.search(res_pattern, cover_url)
        if match:
            # replace the hardcoded resolution with dynamic one
            cover_url = re.sub(res_pattern, '{w}x{h}', cover_url)

        # replace the dynamic_uri h and w parameter with the wanted size
        return cover_url.format(w=size, h=size)

    def get_track_info(self, track_id: str, quality_tier: QualityEnum, codec_options: CodecOptions, slug: str = None,
                       data=None, **extra_kwargs) -> TrackInfo:
        """Get track info with support for release charts"""
        # Handle release chart downloads
        if extra_kwargs.get('is_release_chart'):
            releases = extra_kwargs.get('releases', [])
            release = next((r for r in releases if r['id'] == track_id), None)
            if release:
                # This is actually a release, not a track
                # Get album info and return first track info
                album_info = self.get_album_info(track_id)
                if album_info.tracks:
                    return self.get_track_info(
                        album_info.tracks[0],
                        quality_tier,
                        codec_options,
                        data=data
                    )
        
        # Regular track info handling
        if data is None:
            data = {}

        try:
            track_data = data[track_id] if track_id in data else self.session.get_track(track_id)
        except ConnectionError as e:
            error_msg = str(e)
            if "Territory Restricted" in error_msg:
                error = f"Track {track_id} is not available in your region"
            elif '"detail":"Not found."' in error_msg:
                error = f"Track {track_id} no longer exists or has been removed"
            else:
                error = f"Failed to get track {track_id}: {error_msg}"
                
            # Return a placeholder TrackInfo for any error case
            return TrackInfo(
                name=f"Failed Track ({track_id})",
                album="Unknown Album",
                album_id=None,
                artists=["Unknown Artist"],
                artist_id=None,
                release_year=None,
                duration=None,
                bitrate=None,
                bit_depth=None,
                sample_rate=None,
                cover_url=None,
                tags=Tags(),
                codec=CodecEnum.AAC,  # Set a default codec
                download_extra_kwargs=None,
                error=error
            )
            
        album_id = track_data.get('release').get('id')
        album_data = {}
        error = None

        try:
            album_data = data[album_id] if album_id in data else self.session.get_release(album_id)
        except ConnectionError as e:
            # check if the album is region locked
            if 'Territory Restricted.' in str(e):
                error = f"Album {album_id} is region locked"

        track_name = track_data.get('name')
        track_name += f' ({track_data.get("mix_name")})' if track_data.get("mix_name") else ''

        release_year = track_data.get('publish_date')[:4] if track_data.get('publish_date') else None
        genres = [track_data.get('genre').get('name')]
        # check if a second genre exists
        genres += [track_data.get('sub_genre').get('name')] if track_data.get('sub_genre') else []

        extra_tags = {}
        if track_data.get('bpm'):
            extra_tags['BPM'] = str(track_data.get('bpm'))
        if track_data.get('key'):
            extra_tags['KEY'] = track_data.get('key').get('name')

        tags = Tags(
            album_artist=album_data.get('artists', [{}])[0].get('name'),
            track_number=track_data.get('number'),
            total_tracks=album_data.get('track_count'),
            upc=album_data.get('upc'),
            isrc=track_data.get('isrc'),
            genres=genres,
            release_date=track_data.get('publish_date'),
            copyright=f'© {release_year} {track_data.get("release").get("label").get("name")}',
            label=track_data.get('release').get('label').get('name'),
            extra_tags=extra_tags
        )

        if not track_data['is_available_for_streaming']:
            error = f'Track "{track_data.get("name")}" is not streamable!'
        elif track_data.get('preorder'):
            error = f'Track "{track_data.get("name")}" is not yet released!'
        elif track_data.get('territory_restricted'):  # Add check for territory restriction
            error = f'Track "{track_data.get("name")}" is not available in your region'

        quality = self.quality_parse[quality_tier]
        # Update bitrate mapping to match our quality levels
        bitrate = {
            "low": 128,      # 128k AAC
            "medium": 128,   # 128k AAC
            "high": 256,     # 256k AAC
            "flac": 1411     # FLAC
        }
        length_ms = track_data.get('length_ms')

        track_info = TrackInfo(
            name=track_name,
            album=album_data.get('name'),
            album_id=album_data.get('id'),
            artists=[a.get('name') for a in track_data.get('artists')],
            artist_id=track_data.get('artists')[0].get('id'),
            release_year=release_year,
            duration=length_ms // 1000 if length_ms else None,
            bitrate=bitrate[quality],
            bit_depth=16 if quality == "flac" else None,
            sample_rate=44.1,
            cover_url=self._generate_artwork_url(
                track_data.get('release').get('image').get('dynamic_uri'), self.cover_size),
            tags=tags,
            codec=CodecEnum.AAC if quality_tier not in {QualityEnum.HIFI, QualityEnum.LOSSLESS} else CodecEnum.FLAC,
            download_extra_kwargs={'track_id': track_id, 'quality_tier': quality_tier},
            error=error
        )

        return track_info

    def get_track_cover(self, track_id: str, cover_options: CoverOptions, data=None) -> CoverInfo:
        if data is None:
            data = {}

        track_data = data[track_id] if track_id in data else self.session.get_track(track_id)
        cover_url = track_data.get('release').get('image').get('dynamic_uri')

        return CoverInfo(
            url=self._generate_artwork_url(cover_url, cover_options.resolution),
            file_type=ImageFileTypeEnum.jpg)

    def get_track_download(self, track_id: str, quality_tier: QualityEnum) -> TrackDownloadInfo:
        """Get track download info"""
        try:
            # First check if track is available
            try:
                track_data = self.session.get_track(track_id)
                if track_data.get('territory_restricted'):
                    raise self.exception('Track is not available in your region')
            except ConnectionError as e:
                if "Territory Restricted" in str(e):
                    raise self.exception('Track is not available in your region')
                raise e

            # Function to get download URL with fresh token
            def get_fresh_download_url():
                self.print("Refreshing token...")
                refresh_data = self.session.refresh()
                if refresh_data.get('error'):
                    raise self.exception(f"Failed to refresh token: {refresh_data.get('error')}")
                
                # Get new download URL after refresh
                download_data = self.session.get_track_download(track_id, quality=quality)
                if not download_data or not download_data.get('download_url'):
                    raise self.exception('Could not get download URL after token refresh')
                return download_data['download_url']

            # Check if we need to refresh the token
            if self.session.expires and datetime.now() >= self.session.expires - timedelta(minutes=5):
                self.print("Access token expiring soon, refreshing...")
                download_url = get_fresh_download_url()
            else:
                # Map quality tier and get download URL
                quality = self.quality_parse[quality_tier]
                download_data = self.session.get_track_download(track_id, quality=quality)
                
                if not download_data or not download_data.get('download_url'):
                    raise self.exception('Could not get download URL')
                
                download_url = download_data['download_url']
            
            # Make a HEAD request to check the download URL
            response = self.session.s.head(download_url)
            
            # If we get a 401/403/404, try refreshing the token and retry
            retries = 2  # Number of retries after token refresh
            while response.status_code in (401, 403, 404) and retries > 0:
                self.print(f"Download URL returned {response.status_code}, refreshing token and retrying...")
                download_url = get_fresh_download_url()
                response = self.session.s.head(download_url)
                retries -= 1
            
            # Check if response is valid
            if response.status_code != 200:
                raise self.exception(f'Download URL returned error status {response.status_code}')
            
            # Check content length (should be more than a few KB at least)
            content_length = int(response.headers.get('content-length', 0))
            if content_length < 1000000:  # Less than 1MB is suspicious for a music file
                raise self.exception(f'Invalid file size for music track ({content_length} bytes)')
            
            # Check content type - be more lenient with content type checks
            content_type = response.headers.get('content-type', '').lower()
            if content_type:  # Only check if content type is present
                expected_types = ['audio', 'application/octet-stream', 'binary']
                if not any(expected in content_type for expected in expected_types):
                    self.print(f"Warning: Unexpected content type: {content_type}")
                    # Don't fail here, some CDNs might report different content types

            return TrackDownloadInfo(
                download_type=DownloadEnum.URL,
                file_url=download_url,
                different_codec=CodecEnum.AAC if quality != 'flac' else CodecEnum.FLAC
            )

        except Exception as e:
            if isinstance(e, self.exception):
                raise e
            raise self.exception(f'Download failed: {str(e)}')

    def get_album_info(self, album_id: str, data=None, is_chart: bool = False) -> AlbumInfo:
        """Get album info and its tracks"""
        # check if album is already in album cache
        if data is None:
            data = {}

        # Get album data
        album_data = data.get(album_id) if album_id in data else self.session.get_release(album_id)
        
        # Get track IDs first
        tracks_data = self.session.get_release_tracks(album_id)
        tracks = tracks_data.get('results', [])
        
        # Get total tracks count
        total_tracks = tracks_data.get('count', 0)
        
        # Fetch remaining tracks if any
        for page in range(2, (total_tracks - 1) // 100 + 2):
            print(f'Fetching {len(tracks)}/{total_tracks}', end='\r')
            more_tracks = self.session.get_release_tracks(album_id, page=page)
            tracks.extend(more_tracks.get('results', []))

        # Create cache for track data
        cache = {'data': {album_id: album_data}}
        for i, track in enumerate(tracks):
            # add the track numbers
            track['number'] = i + 1
            # add the modified track to the track_extra_kwargs
            cache['data'][track.get('id')] = track

        return AlbumInfo(
            name=album_data.get('name'),
            release_year=album_data.get('publish_date')[:4] if album_data.get('publish_date') else None,
            duration=sum([t.get('length_ms', 0) // 1000 for t in tracks]),
            upc=album_data.get('upc'),
            cover_url=self._generate_artwork_url(album_data.get('image').get('dynamic_uri'), self.cover_size),
            artist=album_data.get('artists')[0].get('name'),
            artist_id=album_data.get('artists')[0].get('id'),
            tracks=[t.get('id') for t in tracks],
            track_extra_kwargs=cache
        )

    def _init_db(self):
        """Initialize SQLite database for release tracking"""
        db_path = os.path.join(os.path.dirname(__file__), 'releases.db')
        conn = sqlite3.connect(db_path)
        c = conn.cursor()
        
        # Create tables if they don't exist
        c.execute('''CREATE TABLE IF NOT EXISTS charts
                     (id INTEGER PRIMARY KEY AUTOINCREMENT,
                      url TEXT UNIQUE,
                      name TEXT,
                      date TEXT,
                      data TEXT,  -- JSON string of releases data
                      last_updated TIMESTAMP)''')
        
        conn.commit()
        return conn

    def _get_cached_chart(self, url: str) -> tuple[bool, list]:
        """Check if we have a valid cached version of the chart"""
        conn = self._init_db()
        c = conn.cursor()
        
        # Get chart data not older than 7 days
        seven_days_ago = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d %H:%M:%S')
        c.execute('''SELECT data FROM charts 
                     WHERE url = ? AND last_updated > ?''', 
                  (url, seven_days_ago))
        
        result = c.fetchone()
        conn.close()
        
        if result:
            import json
            return True, json.loads(result[0])
        return False, []

    def _save_chart_cache(self, url: str, name: str, releases: list):
        """Save chart data to cache"""
        conn = self._init_db()
        c = conn.cursor()
        
        import json
        # Use REPLACE to handle both insert and update
        c.execute('''REPLACE INTO charts 
                     (url, name, date, data, last_updated)
                     VALUES (?, ?, ?, ?, ?)''',
                  (url, name, 
                   datetime.now().strftime('%Y-%m-%d'),
                   json.dumps(releases),
                   datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
        
        conn.commit()
        conn.close()

    def _get_releases_chart_info(self, url: str) -> PlaylistInfo:
        """Handle scraping of top 100 releases charts"""
        # Check cache first
        is_cached, cached_releases = self._get_cached_chart(url)
        if is_cached:
            self.print("Using cached chart data (less than 7 days old)")
            releases = cached_releases
        else:
            self.print("No recent cache found, scraping chart data...")
            from selenium import webdriver
            from selenium.webdriver.common.by import By
            from selenium.webdriver.support.ui import WebDriverWait
            from selenium.webdriver.support import expected_conditions as EC
            import time
            
            self.print("Initializing Chrome driver for releases chart...")
            chrome_options = webdriver.ChromeOptions()
            chrome_options.add_argument('--headless')
            chrome_options.add_argument('--disable-gpu')
            chrome_options.add_argument('--no-sandbox')
            chrome_options.add_argument('--disable-dev-shm-usage')
            chrome_options.add_argument("--window-size=1920,1080")
            
            driver = None
            try:
                driver = webdriver.Chrome(options=chrome_options)
                self.print(f"Navigating to releases chart URL: {url}")
                driver.get(url)
                time.sleep(5)
                
                # Wait for any release link to appear
                self.print("Looking for releases...")
                WebDriverWait(driver, 20).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "a.artwork[href*='/release/']"))
                )
                
                # Get all unique release IDs first
                release_links = driver.find_elements(By.CSS_SELECTOR, "a.artwork[href*='/release/']")
                seen_ids = set()
                releases = []
                
                for link in release_links:
                    if len(releases) >= 100:  # Stop after 100 unique releases
                        break
                        
                    release_url = link.get_attribute("href")
                    release_id = release_url.split("/")[-1]
                    
                    if release_id in seen_ids:  # Skip duplicates
                        continue
                        
                    seen_ids.add(release_id)
                    
                    try:
                        # Get release name from the title
                        release_name = link.get_attribute("title")
                        if not release_name:
                            # Try to find name in parent element
                            parent = link.find_element(By.XPATH, "./..")
                            name_element = parent.find_element(By.CSS_SELECTOR, "span[class*='ReleaseName']")
                            release_name = name_element.text.strip()
                        
                        if not release_name:
                            release_name = f"Release {release_id}"
                        
                        # Get release info to get track count
                        release_data = self.session.get_release(release_id)
                        track_count = release_data.get('track_count', 0)
                        
                        releases.append({
                            'id': release_id,
                            'name': release_name,
                            'track_count': track_count
                        })
                        self.print(f"Found release: {release_name} (ID: {release_id}, Tracks: {track_count})")
                        
                    except Exception as e:
                        self.print(f"Error processing release {release_id}: {str(e)}")
                        continue
                
                if driver:
                    driver.quit()
                
                if not releases:
                    raise self.exception("No releases found in chart")
                
                self.print(f"\nFound {len(releases)} unique releases")
                
                # Save to cache after successful scrape
                genre = url.split("/genre/")[1].split("/")[0].replace("-", " ").title()
                chart_name = f"Beatport {genre} Top 100 Releases [{datetime.now().strftime('%Y-%m-%d')}]"
                self._save_chart_cache(url, chart_name, releases)
            
            except Exception as e:
                if driver:
                    driver.quit()
                raise e
        
        # Create playlist info structure with tracks list containing release IDs
        genre = url.split("/genre/")[1].split("/")[0].replace("-", " ").title()
        chart_name = f"Beatport {genre} Top 100 Releases [{datetime.now().strftime('%Y-%m-%d')}]"
        
        return PlaylistInfo(
            name=chart_name,
            creator="Beatport",
            release_year=datetime.now().year,
            duration=0,
            # Add release IDs as tracks
            tracks=[release['id'] for release in releases],
            cover_url=None,
            track_extra_kwargs={
                'data': {},
                'is_release_chart': True,
                'releases': releases,
                'chart_name': chart_name
            }
        )

    def download_playlist(self, playlist_id: str, **extra_kwargs):
        """Handle both regular playlists and release charts"""
        playlist_info = self.get_playlist_info(playlist_id, **extra_kwargs)
        
        # Check if this is a releases chart
        if playlist_info.track_extra_kwargs.get('is_release_chart'):
            releases = playlist_info.track_extra_kwargs.get('releases', [])
            total_releases = len(releases)
            
            for i, release in enumerate(releases, 1):
                try:
                    self.print(f"\n=== Downloading Beatport Release {i}/{total_releases}: {release['name']} ({release['track_count']} tracks) ===")
                    
                    # Create folder for this release directly in downloads
                    release_folder = os.path.join('downloads', f"{release['name']} ({datetime.now().strftime('%d-%m-%Y')})")
                    os.makedirs(release_folder, exist_ok=True)
                    
                    # Get album info and download all tracks
                    album_info = self.get_album_info(release['id'])
                    
                    for j, track_id in enumerate(album_info.tracks, 1):
                        try:
                            self.print(f"    Downloading track {j}/{len(album_info.tracks)}")
                            self.download_track(
                                track_id,
                                album_location=release_folder,
                                track_extra_kwargs=album_info.track_extra_kwargs
                            )
                        except Exception as e:
                            self.print(f"Failed to download track {track_id}: {str(e)}")
                            continue
                            
                except Exception as e:
                    self.print(f"Failed to process release {release['id']}: {str(e)}")
                    continue
                
            return
            
        # Handle regular playlists
        # Create folder with download date
        playlist_folder = os.path.join('downloads', f"{playlist_info.name} ({datetime.now().strftime('%d-%m-%Y')})")
        os.makedirs(playlist_folder, exist_ok=True)
        
        for track_id in playlist_info.tracks:
            try:
                self.download_track(
                    track_id,
                    album_location=playlist_folder,
                    track_extra_kwargs=playlist_info.track_extra_kwargs
                )
            except Exception as e:
                self.print(f"Failed to download track {track_id}: {str(e)}")
                continue

    def _process_chart_releases(self, chart_id: int, base_path: str):
        """Process releases from database"""
        conn = self._init_db()
        c = conn.cursor()
        
        # Get all pending releases for this chart
        c.execute('''SELECT id, name, track_count, position 
                     FROM releases 
                     WHERE chart_id = ? AND status = 'pending'
                     ORDER BY position''', (chart_id,))
        releases = c.fetchall()
        
        total_releases = len(releases)
        for release in releases:
            release_id, name, track_count, position = release
            try:
                self.print(f"\nProcessing release {position}/{total_releases}: {name} ({track_count} tracks)")
                
                # Create subfolder for release
                release_folder = os.path.join(base_path, f"{position:02d}. {name}")
                os.makedirs(release_folder, exist_ok=True)
                
                # Download using regular album download
                album_info = self.get_album_info(release_id)
                success = True
                
                for track_id in album_info.tracks:
                    try:
                        track_info = self.get_track_info(
                            track_id,
                            self.quality_tier,
                            self.codec_options,
                            data=album_info.track_extra_kwargs.get('data', {})
                        )
                        
                        if track_info.error:
                            self.print(f"Error with track {track_id}: {track_info.error}")
                            success = False
                            continue
                            
                        self.download_track(
                            track_id,
                            album_location=release_folder,
                            track_extra_kwargs=album_info.track_extra_kwargs
                        )
                    except Exception as e:
                        self.print(f"Failed to download track {track_id}: {str(e)}")
                        success = False
                        continue
                
                # Update release status
                c.execute('UPDATE releases SET status = ? WHERE id = ?',
                         ('completed' if success else 'failed', release_id))
                conn.commit()
                
            except Exception as e:
                self.print(f"Failed to process release {release_id}: {str(e)}")
                c.execute('UPDATE releases SET status = ? WHERE id = ?',
                         ('failed', release_id))
                conn.commit()
                continue
        
        # Update chart status
        c.execute('UPDATE charts SET status = ? WHERE id = ?', ('completed', chart_id))
        conn.commit()
        conn.close()

    def download_track(self, track_id: str, album_location: str = None, track_extra_kwargs=None):
        """Handle both regular tracks and releases from charts"""
        if track_extra_kwargs and track_extra_kwargs.get('is_release_chart'):
            # This is a release from a chart
            releases = track_extra_kwargs.get('releases', [])
            release = next((r for r in releases if r['id'] == track_id), None)
            if release:
                # Create folder for this release directly in downloads
                release_folder = os.path.join('downloads', f"{release['name']}")
                os.makedirs(release_folder, exist_ok=True)
                
                self.print(f"\n=== Downloading Beatport Release: {release['name']} ({release['track_count']} tracks) ===")
                
                # Download the release using album download
                album_info = self.get_album_info(track_id)
                for i, album_track_id in enumerate(album_info.tracks, 1):
                    try:
                        track_info = self.get_track_info(
                            album_track_id,
                            self.quality_tier,
                            self.codec_options,
                            data=album_info.track_extra_kwargs.get('data', {})
                        )
                        
                        if track_info.error:
                            self.print(f"Error with track {album_track_id}: {track_info.error}")
                            continue
                        
                        self.print(f"    Track {i}/{len(album_info.tracks)}")
                        super().download_track(
                            album_track_id,
                            album_location=release_folder,
                            track_extra_kwargs=album_info.track_extra_kwargs
                        )
                    except Exception as e:
                        self.print(f"Failed to download track {album_track_id}: {str(e)}")
                        continue
                return
        
        # Handle regular track download
        super().download_track(track_id, album_location, track_extra_kwargs)