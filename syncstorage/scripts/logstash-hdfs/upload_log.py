#!/usr/bin/python
from ConfigParser import SafeConfigParser
from metlog.config import client_from_dict_config
import argparse
import os
import os.path
import shutil
import subprocess
import sys
import time


class readable_dir(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        prospective_dir = values
        if not os.path.isdir(prospective_dir):
            msg = "readable_dir:{0} is not a valid path"
            msg = msg.format(prospective_dir)
            raise argparse.ArgumentTypeError(msg)
        if os.access(prospective_dir, os.R_OK):
            setattr(namespace, self.dest, os.path.abspath(prospective_dir))
        else:
            msg = "readable_dir:{0} is not a readable dir"
            msg = msg.format(prospective_dir)
            raise argparse.ArgumentTypeError(msg)


class HDFSUploader(object):
    def __init__(self, cfg, ssh_keys):
        self._cfg = cfg
        self._ssh_keypath = ssh_keys

        print "SSH Keys are in : %s" % self._ssh_keypath

        self.HADOOP_USER = cfg.get('metlog_metrics_hdfs', 'HADOOP_USER')
        self.HADOOP_HOST = cfg.get('metlog_metrics_hdfs', 'HADOOP_HOST')
        self.SRC_LOGFILE = cfg.get('metlog_metrics_hdfs', 'SRC_LOGFILE')

        self.DST_FNAME = os.path.join(os.path.split(
                                      cfg.get('metlog_metrics_hdfs',
                                              'DST_FNAME'))[0],
                                      os.path.split(time.strftime(
                                                    self.SRC_LOGFILE))[-1])

        # Make a copy of the log file in case it gets rotated out from
        # under us
        self.TMP_DIR = cfg.get('metlog_metrics_hdfs', 'TMP_DIR')
        self.LOCAL_FNAME = os.path.join(self.TMP_DIR,
                                        os.path.split(self.DST_FNAME)[-1])

        self.ERR_RM_HDFS = 'Failed to remove [%s] from %s'
        self.ERR_RM_HDFS = self.ERR_RM_HDFS % (self.DST_FNAME,
                                               self.HADOOP_HOST)

        self.ERR_XFER_HADOOP = "Transport of [%s] to HDFS failed"
        self.ERR_XFER_HADOOP = self.ERR_XFER_HADOOP % self.DST_FNAME

        self.ERR_REMOVE_LOCAL = "Error removing: [%s]" % self.LOCAL_FNAME

        self.ERR_DFS_WRITE = "DFS Write failure for [%s]" % self.DST_FNAME

        self.LOGGER = client_from_dict_config(dict(cfg.items('metlog')))

    def remove_file_from_hadoop(self):
        priv_key = os.path.join(self._ssh_keypath,
                                "id_private_%s" % self.HADOOP_USER)
        ssh_target = "%s@%s" % (self.HADOOP_USER, self.HADOOP_HOST)
        rm_cmd = ["/usr/bin/ssh",
                  "-i",
                  priv_key,
                  ssh_target,
                  "rm",
                  self.DST_FNAME]
        # try to clean up the file off of the metrics server
        self.LOGGER.info(' '.join(rm_cmd))
        rm_result = self.call_subprocess(rm_cmd)

        if rm_result != 0:
            self.LOGGER.error(self.ERR_RM_HDFS)

    def cleanup_local(self):
        try:
            os.remove(self.LOCAL_FNAME)
        except:
            self.LOGGER.error(self.ERR_REMOVE_LOCAL)

    def copy_log_local(self):
        try:
            src_file = time.strftime(self.SRC_LOGFILE)
            shutil.copy(src_file, self.LOCAL_FNAME)
        except:
            self.LOGGER.exception("Error copying JSON log file for processing")
            sys.exit(1)

    def push_to_hadoop_host(self):
        priv_key = os.path.join(self._ssh_keypath,
                                "id_private_%s" % self.HADOOP_USER)

        scp_remote = "%s@%s:%s" % (self.HADOOP_USER,
                                   self.HADOOP_HOST,
                                   self.DST_FNAME)

        scp_cmd = ["/usr/bin/scp",
                   "-i",
                   priv_key,
                   self.LOCAL_FNAME,
                   scp_remote]
        self.LOGGER.info(' '.join(scp_cmd))
        scp_result = self.call_subprocess(scp_cmd)

        if scp_result != 0:
            self.LOGGER.error(self.ERR_XFER_HADOOP)
            sys.exit(1)

    def call_subprocess(self, *args, **kwargs):
        return subprocess.call(*args, **kwargs)

    def dfs_put(self):
        # Just tell hadoop to import the file
        priv_key = os.path.join(self._ssh_keypath,
                                "id_private_%s" % self.HADOOP_USER)
        ssh_target = "%s@%s" % (self.HADOOP_USER, self.HADOOP_HOST)

        cmd = ["/usr/bin/ssh",
               "-i",
               priv_key,
               ssh_target,
               "hadoop",
               "dfs",
               "-put",
               self.DST_FNAME,
               "/user/%s/%s" % (self.HADOOP_USER, self.DST_FNAME)]

        self.LOGGER.info(' '.join(cmd))
        dfs_result = self.call_subprocess(cmd)

        if dfs_result != 0:
            self.remove_file_from_hadoop()
            self.LOGGER.error(self.ERR_DFS_WRITE)
            sys.exit(1)
        else:
            self.remove_file_from_hadoop()
            self.cleanup_local()


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Upload JSON logs to HDFS")
    parser.add_argument('--config',
                        type=argparse.FileType('r'),
                        required=True)

    parser.add_argument('--ssh-keys',
                        dest='ssh_keys',
                        action=readable_dir, required=True)

    parsed_args = parser.parse_args()

    cfg = SafeConfigParser()
    cfg.readfp(parsed_args.config)

    uploader = HDFSUploader(cfg, parsed_args.ssh_keys)
    uploader.copy_log_local()
    uploader.push_to_hadoop_host()
    uploader.dfs_put()

if __name__ == '__main__':
    main()
