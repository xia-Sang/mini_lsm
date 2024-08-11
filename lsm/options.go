package lsm

import (
	"errors"
	"path"

	"github.com/xia-Sang/lsm_go/util"
)

var ErrorNotExist = errors.New("key not exist")

type Options struct {
	dirPath     string //配置文件
	maxSSTSize  int    //sst size
	maxLevel    int    //最大等级
	maxLevelNum int    //每一层最多sst数量
	tableNum    int    // 一个sst 里面有block的个数
}

type Option func(*Options)

func WithMaxSSTSize(size int) Option {
	return func(o *Options) {
		o.maxSSTSize = size
	}
}

func WithMaxLevel(level int) Option {
	return func(o *Options) {
		o.maxLevel = level
	}
}

func WithMaxLevelNum(num int) Option {
	return func(o *Options) {
		o.maxLevelNum = num
	}
}

func WithTableNum(num int) Option {
	return func(o *Options) {
		o.tableNum = num
	}
}
func (o *Options) defaultOptions() {
	if o.maxLevelNum <= 0 {
		o.maxLevelNum = 7
	}
	if o.tableNum <= 0 {
		o.tableNum = 10
	}
	if o.maxLevel <= 0 {
		o.maxLevel = 7
	}
	if o.maxSSTSize <= 0 {
		o.maxSSTSize = 1024
	}
}
func NewOptions(dirPath string, opts ...Option) (*Options, error) {
	options := &Options{dirPath: dirPath}

	for _, opt := range opts {
		opt(options)
	}

	options.defaultOptions()

	return options, options.check()
}
func (o *Options) check() error {
	if err := util.MakeDirPath(o.dirPath); err != nil {
		return err
	}
	if err := util.MakeDirPath(path.Join(o.dirPath, WalFileName)); err != nil {
		return err
	}
	return nil
}
