import logging

from nshm_toshi_client import toshi_client_base  # noqa: E402

log = logging.getLogger(__name__)


class ApiClient(toshi_client_base.ToshiClientBase):

    def get_gt_subtasks(self, id):
        qry = '''
            query general ($id:ID!)  {
              node(id: $id) {
                __typename
                ... on GeneralTask {
                  title
                  description
                  created
                  children {
                    total_count
                    edges {
                      node {
                        child {
                          __typename
                          ... on Node {
                            id
                          }
                        }
                      }
                    }
                  }
                }
              }
            }'''

        log.debug(qry)
        input_variables = dict(id=id)
        executed = self.run_query(qry, input_variables)
        return executed['node']

    def get_oq_hazard_task(self, id):
        """
        node(id: "T3BlbnF1YWtlSGF6YXJkVGFzazoxMzI4NDE3") { # "2023-03-20T "Source Logic Tree v8.0.2", -> T3BlbnF1YWtlSGF6YXJkVGFzazoxMzI4NDE3
        node(id:"T3BlbnF1YWtlSGF6YXJkVGFzazo2NTM3Mjcy") { # "2023-08-21T "Source Logic Tree v9.0.0", -> T3BlbnF1YWtlSGF6YXJkVGFzazo2NTM3Mjcy
        node(id: "T3BlbnF1YWtlSGF6YXJkVGFzazo2NzAxMjU1") { # "2024-01-31T "Logic Tree 9.0.1, locations for cave locations", -> T3BlbnF1YWtlSGF6YXJkVGFzazo2NzAxMjU1
        """  # noqa
        qry = '''
            query oqht ($id:ID!)  {
                node(id: $id) {
                   ... on OpenquakeHazardTask {
                    created
                    id
                    result
                    duration
                    task_type
                    model_type
                    hazard_solution {
                      ... on Node {
                        id
                        __typename
                      }
                      hdf5_archive {
                        file_name
                        file_size
                        file_url
                      }
                      task_args {
                        file_name
                        file_size
                        file_url
                      }
                      config {
                        id
                        created

                        files {
                          edges {
                            node {
                              file {
                                ... on FileInterface {
                                  file_name
                                  file_size
                                  file_url
                                }
                              }
                            }
                          }
                        }
                        template_archive {
                          id
                          meta {
                            k
                            v
                          }
                          file_url
                          file_name
                          file_size

                        }
                        created
                      }
                      meta {
                        k
                        v
                      }
                    }
                    environment {
                      k
                      v
                    }
                    arguments {
                      k
                      v
                    }
                  }
                 }
                }'''

        log.debug(qry)
        input_variables = dict(id=id)
        executed = self.run_query(qry, input_variables)
        return executed['node']
