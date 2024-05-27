"""Retrieve NSHM docker image metadata from the AWS ECR repos.

NSHM pipeline used a number of different versions/builds of openquake throughout the 2022/23 processing period. These
builds were packaged as docker images and stored in an AWS Elastic Container Repository registry.

The registry allow AWS batch jobs to access a docker image containing the correct components needed
to build hazard relisation or disaggregation curves.

The docker imges themselves carry metadata about the openquake configuration used at the time.
"""

from datetime import datetime, timezone
from functools import partial
from itertools import cycle, groupby
from operator import itemgetter
from typing import Dict, Optional

import boto3
from botocore.config import Config

OPENQUAKE_ECR_REPO_URI = '461564345538.dkr.ecr.us-east-1.amazonaws.com/nzshm22/runzi-openquake'

REGISTRY_ID = '461564345538.dkr.ecr.us-east-1.amazonaws.com'
REPONAME = "nzshm22/runzi-openquake"

aws_config = Config(region_name='us-east-1')
# ecr_client = boto3.client('ecr', config=aws_config)


def chunks(iterable, size=10):
    # see https://stackoverflow.com/a/34935239
    c = cycle((False,) * size + (True,) * size)  # Make a cheap iterator that will group in groups of size elements
    # groupby will pass an element to next as its second argument, but because
    # c is an infinite iterator, the second argument will never get used
    return map(itemgetter(1), groupby(iterable, partial(next, c)))


def get_repository_images(ecr_client, reponame, batch_size=50):
    nextToken = None
    args = dict(repositoryName=reponame, maxResults=batch_size)

    while True:
        if nextToken:
            args['nextToken'] = nextToken
        response = ecr_client.list_images(**args)
        nextToken = response.get('nextToken')
        for image_info in response['imageIds']:
            yield image_info

        if not nextToken:
            break


def get_image_info(ecr_client, reponame, image_ids, since: Optional[datetime] = None):

    nextToken = None
    args = dict(repositoryName=reponame, imageIds=image_ids)

    while True:
        if nextToken:
            args['nextToken'] = nextToken

        response = ecr_client.describe_images(**args)
        nextToken = response.get('nextToken')
        for image_info in response['imageDetails']:
            if image_info['imagePushedAt'] >= since:
                yield image_info
        if not nextToken:
            break


def process_repo_images(ecr_client, reponame, since: Optional[datetime] = None):
    images = get_repository_images(ecr_client, reponame)
    for chunk in chunks(images, 10):
        image_infos = list(chunk)
        # print(image_infos)
        for image in get_image_info(ecr_client, REPONAME, image_infos, since):
            yield image


class ECRRepoStash:

    def __init__(self, reponame, oldest_image_date: datetime, ecr_client=None):
        self._client = ecr_client or boto3.client('ecr', config=aws_config)
        self._reponame = reponame
        self._oldest_image = oldest_image_date or datetime(2022, 1, 1)
        self._since_date_mapping: Dict[str, Dict] = {}

    def fetch(self):
        self._since_date_mapping = {}
        for repo_image in process_repo_images(self._client, self._reponame, self._oldest_image):
            self._since_date_mapping[repo_image['imagePushedAt']] = repo_image
        return self

    @property
    def sorted_since(self):
        return sorted(self._since_date_mapping.keys())

    @property
    def images(self):
        for key in self.sorted_since:
            yield self._since_date_mapping[key]
        # for image in self._since_date_mapping.items():
        #     yield image

    def active_image_asat(self, since: datetime):
        for d in reversed(self.sorted_since):
            if d < since:
                return self._since_date_mapping[d]


if __name__ == "__main__":
    # get list of images
    since = datetime(2023, 3, 20, tzinfo=timezone.utc)

    rs = ECRRepoStash(REPONAME, since)
    rs.fetch()
    print(len(list(rs.images)))
    print()
    print(rs.active_image_asat(datetime(2024, 1, 28, tzinfo=timezone.utc)))

    print(rs.active_image_asat(datetime(2024, 1, 1, tzinfo=timezone.utc)))

    # count = 0
    # since_map = {}
    # for repo_image in process_repo_images(REPONAME, since):
    #     # print(repo_image)
    #     since_map[repo_image['imagePushedAt']] = repo_image
    #     count +=1

    # sorted_since = sorted(since_map.keys())

    # print(f'Counted {count} images since {since}')

    # print(sorted_since)
    # print(since_map)

    print()

    # print(get_prior_image(datetime(2024,2,1, tzinfo=timezone.utc)))
    # print(imgs[-1])
    # print()
    # print(f"got {len(imgs)} images in repo {REPONAME}")

    # #get some details
    # infos = get_image_info(REPONAME, imgs[:3])
    # print()
    # print(list(infos)[0])
